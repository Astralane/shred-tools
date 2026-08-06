//! DoubleZero Edge multicast group membership.
//!
//! This mirrors how raiku-agave receives DoubleZero shreds
//! (`core/src/multicast_shred_receive_service.rs`, commit a855ee9): bind
//! `0.0.0.0:<group port>` with `SO_REUSEADDR`, and manage IGMP membership
//! separately from the bind, gated on the DoubleZero host route.
//!
//! Two properties of that design are what make this tool usable on a live
//! validator at all:
//!
//! * **The bind is INADDR_ANY, not the group address.** One socket then receives
//!   every group on that port — mainnet ships a leader group and a turbine-root
//!   group, both on 7733. What actually arrives is decided by the IGMP
//!   membership, not by the bind.
//! * **`SO_REUSEADDR` on a multicast port means fan-out, not stealing.** The
//!   kernel delivers each multicast datagram to *every* socket bound to that
//!   port, so shred-audit gets its own copy of every shred alongside the
//!   validator's own receive socket. Unlike the unicast TVU port, this leg needs
//!   no mirroring and adds no hop: the timestamp is the kernel's, taken at
//!   driver handoff on the DoubleZero interface, directly comparable with a
//!   turbine timestamp taken the same way.
//!
//! Membership is reconciled against the routing table the way agave reconciles
//! it, and every transition is recorded. That record is not bookkeeping: a
//! tunnel that was down for part of a capture makes DoubleZero look like it lost
//! races it was never in, and the manifest has to say so outright rather than
//! leave the numbers to imply a slow transport.

use std::{
    io, mem,
    net::Ipv4Addr,
    os::fd::{AsRawFd, RawFd},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use ahash::AHashSet;
use anyhow::Result;

use crate::{
    config::MulticastCfg,
    out::{now_unix_ns, MulticastEvent, MulticastGroupStatus, MulticastStatus},
};

/// UDP port shared by all DoubleZero Edge multicast shred groups.
/// `DEFAULT_MULTICAST_SHRED_PORT` in raiku-agave.
pub const DEFAULT_MULTICAST_SHRED_PORT: u16 = 7733;

/// Mainnet leader-broadcast group (`MULTICAST_SHRED_ADDR_MAINNET`).
pub const MAINNET_LEADER_GROUP: Ipv4Addr = Ipv4Addr::new(233, 84, 178, 1);
/// Mainnet turbine-root group (`MULTICAST_ROOT_SHRED_ADDR_MAINNET`).
pub const MAINNET_ROOT_GROUP: Ipv4Addr = Ipv4Addr::new(233, 84, 178, 16);
/// Testnet leader-broadcast group (`MULTICAST_SHRED_ADDR_TESTNET`).
pub const TESTNET_LEADER_GROUP: Ipv4Addr = Ipv4Addr::new(233, 84, 178, 10);
/// Testnet turbine-root group (`MULTICAST_ROOT_SHRED_ADDR_TESTNET`).
pub const TESTNET_ROOT_GROUP: Ipv4Addr = Ipv4Addr::new(233, 84, 178, 12);

/// How often membership is reconciled against the routing table. Matches
/// agave's `CHECK_INTERVAL` so a flap is seen on the same cadence the validator
/// sees it on.
const CHECK_INTERVAL: Duration = Duration::from_secs(60);
/// How often the exit flag is polled, so shutdown is not held for a whole
/// reconcile interval.
const EXIT_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Cap on retained transition events. A flapping tunnel must not grow this
/// without bound over a multi-day capture; the per-group counters stay exact
/// either way, and a note fires when events are dropped.
const MAX_EVENTS: usize = 512;

/// `/proc/net/route` host-route mask (/32), the shape DoubleZero installs.
#[cfg(any(test, target_os = "linux"))]
const HOST_ROUTE_MASK: u32 = u32::MAX;

/// Per-group membership state, as it stands right now.
#[derive(Clone)]
struct GroupState {
    group: Ipv4Addr,
    port: u16,
    interface: Ipv4Addr,
    require_route: bool,
    joined: bool,
    joins: u64,
    leaves: u64,
    join_errors: u64,
    last_error: Option<String>,
    first_joined_at_ns: Option<i64>,
    /// Nanoseconds this group has been joined, accumulated across intervals.
    /// The open interval (if currently joined) is added when read.
    joined_ns: i64,
    joined_since_ns: Option<i64>,
}

impl GroupState {
    fn new(group: Ipv4Addr, cfg: &MulticastCfg) -> Self {
        Self {
            group,
            port: cfg.port,
            interface: cfg.interface,
            require_route: cfg.require_route,
            joined: false,
            joins: 0,
            leaves: 0,
            join_errors: 0,
            last_error: None,
            first_joined_at_ns: None,
            joined_ns: 0,
            joined_since_ns: None,
        }
    }

    /// Total joined time as of `now`, including the interval still open.
    fn joined_ns_at(&self, now: i64) -> i64 {
        self.joined_ns + self.joined_since_ns.map_or(0, |since| now - since)
    }
}

/// Shared membership record. Written by the reconcile threads, read whenever a
/// manifest is built.
#[derive(Default)]
pub struct MembershipLog {
    inner: Mutex<LogInner>,
}

#[derive(Default)]
struct LogInner {
    groups: Vec<GroupState>,
    events: Vec<MulticastEvent>,
    /// Events discarded once `MAX_EVENTS` was reached. Surfaced as a note so a
    /// truncated event list is never mistaken for a quiet tunnel.
    events_dropped: u64,
}

impl MembershipLog {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register every group before any reconcile runs, so a group that never
    /// joins still appears in the manifest. A declared-but-absent group is the
    /// single most misleading thing this comparison can hide.
    fn register(&self, group: Ipv4Addr, cfg: &MulticastCfg) {
        let mut inner = self.inner.lock().unwrap();
        if inner.groups.iter().any(|g| g.group == group && g.port == cfg.port) {
            return;
        }
        inner.groups.push(GroupState::new(group, cfg));
    }

    fn record(&self, group: Ipv4Addr, action: &str, detail: Option<String>) {
        let at = now_unix_ns();
        let mut inner = self.inner.lock().unwrap();
        if let Some(g) = inner.groups.iter_mut().find(|g| g.group == group) {
            match action {
                "join" => {
                    g.joined = true;
                    g.joins += 1;
                    g.first_joined_at_ns.get_or_insert(at);
                    g.joined_since_ns = Some(at);
                }
                "leave" => {
                    g.joined = false;
                    g.leaves += 1;
                    if let Some(since) = g.joined_since_ns.take() {
                        g.joined_ns += at - since;
                    }
                }
                "join_failed" => {
                    g.join_errors += 1;
                    g.last_error = detail.clone();
                }
                _ => {}
            }
        }
        if inner.events.len() >= MAX_EVENTS {
            inner.events_dropped += 1;
        } else {
            inner.events.push(MulticastEvent {
                at_unix_ns: at,
                group: group.to_string(),
                action: action.to_string(),
                detail,
            });
        }
    }

    /// Start a fresh accounting window at `at`, on archive rotation.
    ///
    /// Joined time and the event list are per-archive, not per-run: each archive
    /// is compared against its own window length, so a cumulative `joined_ns`
    /// would exceed the window of every archive after the first and quietly
    /// suppress the "this group was only joined for X% of the window" note — the
    /// one thing the reader most needs on a flapping tunnel. A group that is
    /// joined right now carries its membership into the new window.
    pub fn begin_window(&self, at: i64) {
        let mut inner = self.inner.lock().unwrap();
        for g in inner.groups.iter_mut() {
            g.joined_ns = 0;
            if g.joined {
                g.joined_since_ns = Some(at);
            }
        }
        inner.events.clear();
        inner.events_dropped = 0;
    }

    /// Snapshot for the manifest, covering the current window.
    pub fn snapshot(&self) -> MulticastStatus {
        let now = now_unix_ns();
        let inner = self.inner.lock().unwrap();
        MulticastStatus {
            groups: inner
                .groups
                .iter()
                .map(|g| MulticastGroupStatus {
                    group: g.group.to_string(),
                    port: g.port,
                    interface: g.interface.to_string(),
                    require_route: g.require_route,
                    joined: g.joined,
                    joined_ns: g.joined_ns_at(now),
                    joins: g.joins,
                    leaves: g.leaves,
                    join_errors: g.join_errors,
                    last_error: g.last_error.clone(),
                    first_joined_at_unix_ns: g.first_joined_at_ns,
                })
                .collect(),
            events: inner.events.clone(),
            events_dropped: inner.events_dropped,
        }
    }

    /// True when no configured group has ever been joined — the comparison has
    /// no DoubleZero leg at all, whatever the timing table says.
    pub fn nothing_ever_joined(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        !inner.groups.is_empty() && inner.groups.iter().all(|g| g.first_joined_at_ns.is_none())
    }
}

/// `IP_ADD_MEMBERSHIP` / `IP_DROP_MEMBERSHIP` on a bound socket.
///
/// `interface` is an address, not an index — the same shape agave uses via
/// `UdpSocket::join_multicast_v4`. `Ipv4Addr::UNSPECIFIED` lets the kernel
/// routing table choose, which is what resolves to the DoubleZero interface
/// once its host route for the group exists.
fn set_membership(fd: RawFd, group: Ipv4Addr, interface: Ipv4Addr, opt: libc::c_int) -> io::Result<()> {
    let mreq = libc::ip_mreq {
        imr_multiaddr: libc::in_addr {
            s_addr: u32::from_ne_bytes(group.octets()),
        },
        imr_interface: libc::in_addr {
            s_addr: u32::from_ne_bytes(interface.octets()),
        },
    };
    let rc = unsafe {
        libc::setsockopt(
            fd,
            libc::IPPROTO_IP,
            opt,
            &mreq as *const _ as *const libc::c_void,
            mem::size_of::<libc::ip_mreq>() as libc::socklen_t,
        )
    };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn join(fd: RawFd, group: Ipv4Addr, interface: Ipv4Addr) -> io::Result<()> {
    set_membership(fd, group, interface, libc::IP_ADD_MEMBERSHIP)
}

fn leave(fd: RawFd, group: Ipv4Addr, interface: Ipv4Addr) -> io::Result<()> {
    set_membership(fd, group, interface, libc::IP_DROP_MEMBERSHIP)
}

/// Does the kernel hold a /32 host route to `group`?
///
/// The DoubleZero daemon installs one per group when the Edge tunnel is up, and
/// removes it when the tunnel goes down, so this is the same liveness signal
/// agave gates its membership on. Joining without it would succeed against
/// whatever interface the default route names and then silently receive nothing.
#[cfg(target_os = "linux")]
fn has_route(group: Ipv4Addr) -> bool {
    match std::fs::read_to_string("/proc/net/route") {
        Ok(data) => route_table_contains(&data, group),
        Err(e) => {
            eprintln!("warning: could not read /proc/net/route ({e}); treating the DoubleZero route as absent");
            false
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn has_route(_group: Ipv4Addr) -> bool {
    false
}

/// Parse `/proc/net/route` for an exact host route to `group`. Field 1 is the
/// destination and field 7 the mask, both little-endian hex.
#[cfg(any(test, target_os = "linux"))]
fn route_table_contains(data: &str, group: Ipv4Addr) -> bool {
    let destination = u32::from_le_bytes(group.octets());
    for line in data.lines().skip(1) {
        let mut fields = line.split_whitespace();
        let Some(dest) = fields.nth(1) else { continue };
        let Some(mask) = fields.nth(5) else { continue };
        if parse_route_field(dest) == Some(destination)
            && parse_route_field(mask) == Some(HOST_ROUTE_MASK)
        {
            return true;
        }
    }
    false
}

#[cfg(any(test, target_os = "linux"))]
fn parse_route_field(field: &str) -> Option<u32> {
    if field.len() != 8 {
        return None;
    }
    u32::from_str_radix(field, 16).ok()
}

/// Bring membership in line with route presence. Only transitions touch the
/// socket or the log; steady state is a no-op. A failed join is left out of
/// `joined` so the next pass retries it, and a failed leave is still forgotten
/// so a dead socket is not re-left forever. Same contract as agave's.
fn reconcile(
    fd: RawFd,
    groups: &[Ipv4Addr],
    interface: Ipv4Addr,
    require_route: bool,
    joined: &mut AHashSet<Ipv4Addr>,
    log: &MembershipLog,
    route_present: &dyn Fn(Ipv4Addr) -> bool,
) {
    for &group in groups {
        let want = !require_route || route_present(group);
        match (want, joined.contains(&group)) {
            (true, false) => match join(fd, group, interface) {
                Ok(()) => {
                    joined.insert(group);
                    log.record(group, "join", None);
                    eprintln!("multicast: joined {group} on interface {interface}");
                }
                Err(e) => {
                    log.record(group, "join_failed", Some(e.to_string()));
                    eprintln!("warning: failed to join multicast group {group} on {interface}: {e}");
                }
            },
            (false, true) => {
                if let Err(e) = leave(fd, group, interface) {
                    eprintln!("warning: failed to leave multicast group {group} on {interface}: {e}");
                }
                joined.remove(&group);
                log.record(group, "leave", None);
                eprintln!(
                    "multicast: left {group} — its DoubleZero host route is gone, so nothing will \
                     arrive from it until the tunnel returns"
                );
            }
            _ => {}
        }
    }
}

/// Start the membership thread for one multicast port. The socket is already
/// bound; this only manages IGMP.
pub fn spawn_membership(
    socket: Arc<dyn AsRawFd + Send + Sync>,
    cfg: &MulticastCfg,
    log: Arc<MembershipLog>,
    exit: Arc<AtomicBool>,
) -> Result<JoinHandle<()>> {
    let groups = cfg.resolved_groups();
    for &g in &groups {
        log.register(g, cfg);
    }
    let interface = cfg.interface;
    let require_route = cfg.require_route;
    let port = cfg.port;

    Ok(thread::Builder::new()
        .name(format!("mcast-{port}"))
        .spawn(move || {
            let fd = socket.as_raw_fd();
            let mut joined: AHashSet<Ipv4Addr> = AHashSet::new();
            let route_present: Box<dyn Fn(Ipv4Addr) -> bool> = Box::new(has_route);

            reconcile(
                fd,
                &groups,
                interface,
                require_route,
                &mut joined,
                &log,
                route_present.as_ref(),
            );

            let ticks_per_check = (CHECK_INTERVAL.as_secs() / EXIT_POLL_INTERVAL.as_secs()).max(1);
            let mut tick = 0u64;
            while !exit.load(Ordering::Relaxed) {
                thread::sleep(EXIT_POLL_INTERVAL);
                tick += 1;
                if tick >= ticks_per_check {
                    tick = 0;
                    reconcile(
                        fd,
                        &groups,
                        interface,
                        require_route,
                        &mut joined,
                        &log,
                        route_present.as_ref(),
                    );
                }
            }

            // Drop memberships so the capture leaves no state behind. The log
            // keeps the accounting: a leave here closes the joined interval.
            for group in joined.drain() {
                let _ = leave(fd, group, interface);
                log.record(group, "leave", Some("capture ended".to_string()));
            }
        })?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn route_line(dest: &str, mask: &str) -> String {
        format!(
            "Iface\tDestination\tGateway\tFlags\tRefCnt\tUse\tMetric\tMask\tMTU\tWindow\tIRTT\n\
             doublezero0\t{dest}\t00000000\t0001\t0\t0\t0\t{mask}\t0\t0\t0\n"
        )
    }

    /// The destination encoding must match what the kernel writes, or every
    /// group looks routeless and DoubleZero is never joined at all. 233.84.178.1
    /// is 0x01B254E9 — the same vector raiku-agave asserts on.
    #[test]
    fn mainnet_leader_group_route_is_recognised() {
        let data = route_line("01B254E9", "FFFFFFFF");
        assert!(route_table_contains(&data, MAINNET_LEADER_GROUP));
    }

    /// A default route to the group's prefix is not a DoubleZero host route.
    /// Accepting one would join the group over the internet interface and then
    /// receive nothing, which reads as DoubleZero delivering nothing.
    #[test]
    fn only_a_host_route_counts() {
        let data = route_line("01B254E9", "FFFFFF00");
        assert!(!route_table_contains(&data, MAINNET_LEADER_GROUP));
    }

    #[test]
    fn an_unrelated_host_route_does_not_match() {
        let data = route_line("0AB254E9", "FFFFFFFF"); // testnet leader group
        assert!(!route_table_contains(&data, MAINNET_LEADER_GROUP));
        assert!(route_table_contains(&data, TESTNET_LEADER_GROUP));
    }

    #[test]
    fn a_header_only_table_has_no_routes() {
        assert!(!route_table_contains(
            "Iface\tDestination\tGateway\n",
            MAINNET_LEADER_GROUP
        ));
    }

    fn cfg(groups: Vec<Ipv4Addr>) -> MulticastCfg {
        MulticastCfg {
            port: DEFAULT_MULTICAST_SHRED_PORT,
            groups,
            cluster: None,
            interface: Ipv4Addr::UNSPECIFIED,
            require_route: true,
        }
    }

    /// A group whose route never appears must still be in the manifest, flagged
    /// as never joined. Omitting it would leave a capture that received zero
    /// DoubleZero shreds looking like one where DoubleZero simply lost.
    #[test]
    fn a_group_that_never_joined_is_still_reported() {
        let log = MembershipLog::new();
        log.register(MAINNET_LEADER_GROUP, &cfg(vec![MAINNET_LEADER_GROUP]));

        let snap = log.snapshot();
        assert_eq!(snap.groups.len(), 1);
        assert!(!snap.groups[0].joined);
        assert_eq!(snap.groups[0].joins, 0);
        assert!(snap.groups[0].first_joined_at_unix_ns.is_none());
        assert!(log.nothing_ever_joined());
    }

    #[test]
    fn a_flap_is_counted_and_leaves_the_group_joined_again() {
        let log = MembershipLog::new();
        log.register(MAINNET_LEADER_GROUP, &cfg(vec![MAINNET_LEADER_GROUP]));

        log.record(MAINNET_LEADER_GROUP, "join", None);
        log.record(MAINNET_LEADER_GROUP, "leave", None);
        log.record(MAINNET_LEADER_GROUP, "join", None);

        let snap = log.snapshot();
        let g = &snap.groups[0];
        assert_eq!((g.joins, g.leaves), (2, 1));
        assert!(g.joined, "the last transition was a join");
        assert_eq!(snap.events.len(), 3);
        assert!(!log.nothing_ever_joined());
    }

    /// Joined time must accumulate across intervals rather than only tracking
    /// the current one — it is what tells you how much of the capture actually
    /// had a DoubleZero leg.
    #[test]
    fn joined_time_accumulates_across_intervals() {
        let log = MembershipLog::new();
        log.register(MAINNET_LEADER_GROUP, &cfg(vec![MAINNET_LEADER_GROUP]));
        log.record(MAINNET_LEADER_GROUP, "join", None);
        log.record(MAINNET_LEADER_GROUP, "leave", None);
        let closed = log.snapshot().groups[0].joined_ns;

        log.record(MAINNET_LEADER_GROUP, "join", None);
        let reopened = log.snapshot().groups[0].joined_ns;
        assert!(
            reopened >= closed,
            "re-joining must not discard the previously joined interval"
        );
    }

    #[test]
    fn a_failed_join_is_recorded_without_marking_the_group_joined() {
        let log = MembershipLog::new();
        log.register(MAINNET_LEADER_GROUP, &cfg(vec![MAINNET_LEADER_GROUP]));
        log.record(
            MAINNET_LEADER_GROUP,
            "join_failed",
            Some("network is unreachable".to_string()),
        );

        let g = &log.snapshot().groups[0];
        assert!(!g.joined);
        assert_eq!(g.join_errors, 1);
        assert_eq!(g.last_error.as_deref(), Some("network is unreachable"));
    }

    /// The event list is bounded, but a truncated list must announce itself.
    #[test]
    fn events_are_bounded_and_the_overflow_is_counted() {
        let log = MembershipLog::new();
        log.register(MAINNET_LEADER_GROUP, &cfg(vec![MAINNET_LEADER_GROUP]));
        for _ in 0..MAX_EVENTS + 10 {
            log.record(MAINNET_LEADER_GROUP, "join", None);
        }
        let snap = log.snapshot();
        assert_eq!(snap.events.len(), MAX_EVENTS);
        assert_eq!(snap.events_dropped, 10);
    }

    /// Reconcile must not touch the socket when the route is absent, and must
    /// report the failed join when it is present but the join cannot be made
    /// (fd -1 here stands in for an unusable socket).
    #[test]
    fn reconcile_skips_groups_without_a_route() {
        let log = MembershipLog::new();
        let c = cfg(vec![MAINNET_LEADER_GROUP]);
        log.register(MAINNET_LEADER_GROUP, &c);
        let mut joined = AHashSet::new();
        let no_routes = |_: Ipv4Addr| false;

        reconcile(
            -1,
            &[MAINNET_LEADER_GROUP],
            Ipv4Addr::UNSPECIFIED,
            true,
            &mut joined,
            &log,
            &no_routes,
        );

        assert!(joined.is_empty());
        let g = &log.snapshot().groups[0];
        assert_eq!(g.joins, 0);
        assert_eq!(g.join_errors, 0, "no route means no attempt, not a failure");
    }

    #[test]
    fn reconcile_records_a_join_failure_when_the_route_is_present() {
        let log = MembershipLog::new();
        let c = cfg(vec![MAINNET_LEADER_GROUP]);
        log.register(MAINNET_LEADER_GROUP, &c);
        let mut joined = AHashSet::new();
        let all_routes = |_: Ipv4Addr| true;

        reconcile(
            -1, // not a socket: the join must fail and be reported
            &[MAINNET_LEADER_GROUP],
            Ipv4Addr::UNSPECIFIED,
            true,
            &mut joined,
            &log,
            &all_routes,
        );

        assert!(joined.is_empty(), "a failed join must be retried, not remembered");
        assert_eq!(log.snapshot().groups[0].join_errors, 1);
    }

    /// With `require_route: false` the route check is bypassed entirely — the
    /// escape hatch for a setup where DoubleZero installs no host route.
    #[test]
    fn require_route_false_attempts_the_join_regardless() {
        let log = MembershipLog::new();
        let mut c = cfg(vec![MAINNET_LEADER_GROUP]);
        c.require_route = false;
        log.register(MAINNET_LEADER_GROUP, &c);
        let mut joined = AHashSet::new();
        let no_routes = |_: Ipv4Addr| false;

        reconcile(
            -1,
            &[MAINNET_LEADER_GROUP],
            Ipv4Addr::UNSPECIFIED,
            false,
            &mut joined,
            &log,
            &no_routes,
        );

        assert_eq!(
            log.snapshot().groups[0].join_errors,
            1,
            "the join was attempted despite the missing route"
        );
    }
}
