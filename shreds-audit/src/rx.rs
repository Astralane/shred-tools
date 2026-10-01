//! UDP receive path: one `recvmmsg` thread per port. Packets are stamped by the
//! kernel (`SO_TIMESTAMPNS`) before queueing, so our own scheduling delay never
//! leaks into the measurement.

use std::{
    io, mem,
    net::Ipv4Addr,
    os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
    ptr,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
};

use ahash::AHashSet;
use anyhow::{anyhow, Context, Result};
use crossbeam_channel::Sender;

use crate::pinger::NetMon;
use crate::registry::{ProviderId, Registry};

const BATCH: usize = 64;
/// Solana shreds are 1203/1228 bytes.
const MAX_SHRED: usize = 1500;
const CTRL_LEN: usize = 64;

// Not exposed by `libc` on every target; identical on every Linux ABI.
const SO_TIMESTAMPNS: libc::c_int = 35;
const SCM_TIMESTAMPNS: libc::c_int = SO_TIMESTAMPNS;
const SO_RXQ_OVFL: libc::c_int = 40;

pub struct Packet {
    pub provider: ProviderId,
    /// Kernel CLOCK_REALTIME stamp, ns since the unix epoch.
    pub rx_unix_ns: i64,
    pub data: Vec<u8>,
}

#[derive(Default)]
pub struct RxStats {
    pub received: AtomicU64,
    pub unmatched: AtomicU64,
    pub no_timestamp: AtomicU64,
    pub channel_full: AtomicU64,
    /// Kernel drops from a full socket queue (`SO_RXQ_OVFL`): our loss, not the provider's.
    pub kernel_dropped: AtomicU64,
    /// Oversized datagrams cut by the kernel; never parsed, or they'd look like provider defects.
    pub truncated: AtomicU64,
}

pub fn spawn_receivers(
    bind_ip: Ipv4Addr,
    ports: &[u16],
    registry: Arc<Registry>,
    netmon: Arc<NetMon>,
    tx: Sender<Vec<Packet>>,
    stats: Arc<RxStats>,
    exit: Arc<AtomicBool>,
) -> Result<Vec<std::thread::JoinHandle<()>>> {
    let mut handles = Vec::with_capacity(ports.len());
    for &port in ports {
        let sock = bind_socket(bind_ip, port)
            .with_context(|| format!("binding {bind_ip}:{port}"))?;
        let registry = registry.clone();
        let netmon = netmon.clone();
        let tx = tx.clone();
        let stats = stats.clone();
        let exit = exit.clone();
        handles.push(
            std::thread::Builder::new()
                .name(format!("rx-{port}"))
                .spawn(move || rx_loop(sock, port, registry, netmon, tx, stats, exit))?,
        );
    }
    Ok(handles)
}

fn set_opt(fd: RawFd, opt: libc::c_int, val: libc::c_int) -> io::Result<()> {
    let r = unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            opt,
            &val as *const _ as *const libc::c_void,
            mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    if r < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn bind_socket(ip: Ipv4Addr, port: u16) -> Result<OwnedFd> {
    let fd = unsafe { libc::socket(libc::AF_INET, libc::SOCK_DGRAM, 0) };
    if fd < 0 {
        return Err(io::Error::last_os_error().into());
    }
    let sock = unsafe { OwnedFd::from_raw_fd(fd) };

    set_opt(fd, SO_TIMESTAMPNS, 1)
        .map_err(|e| anyhow!("setsockopt(SO_TIMESTAMPNS) failed: {e}"))?;

    if let Err(e) = set_opt(fd, SO_RXQ_OVFL, 1) {
        eprintln!(
            "warning: setsockopt(SO_RXQ_OVFL) failed on port {port}: {e} — kernel receive \
             drops cannot be counted on this socket, and will be indistinguishable from \
             shreds a provider never sent"
        );
    }

    // The kernel silently clamps SO_RCVBUF to net.core.rmem_max, so read it back.
    let want: libc::c_int = 64 * 1024 * 1024;
    if let Err(e) = set_opt(fd, libc::SO_RCVBUF, want) {
        eprintln!("warning: setsockopt(SO_RCVBUF) failed on port {port}: {e}");
    }
    let mut got: libc::c_int = 0;
    let mut len = mem::size_of::<libc::c_int>() as libc::socklen_t;
    let r = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_RCVBUF,
            &mut got as *mut _ as *mut libc::c_void,
            &mut len,
        )
    };
    // Linux reports double what it allotted.
    if r == 0 && (got as i64) < 2 * want as i64 {
        eprintln!(
            "warning: asked for a {} MiB receive buffer on port {port} but the kernel \
             granted {} KiB (clamped by net.core.rmem_max). Bursts will overflow the \
             socket queue; those datagrams are counted as `kernel_drop`, NOT as provider \
             loss, but coverage will be incomplete. Raise it with: \
             sudo sysctl -w net.core.rmem_max={want}",
            want / 1024 / 1024,
            got / 2 / 1024,
        );
    }

    let addr = libc::sockaddr_in {
        sin_family: libc::AF_INET as libc::sa_family_t,
        sin_port: port.to_be(),
        sin_addr: libc::in_addr {
            s_addr: u32::from_ne_bytes(ip.octets()),
        },
        sin_zero: [0; 8],
    };
    let r = unsafe {
        libc::bind(
            fd,
            &addr as *const _ as *const libc::sockaddr,
            mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
        )
    };
    if r < 0 {
        return Err(io::Error::last_os_error().into());
    }
    Ok(sock)
}

/// Scratch buffers for `recvmmsg`, allocated once per thread. The headers point
/// into the Vecs' heap buffers, which never move.
struct RecvArena {
    bufs: Vec<[u8; MAX_SHRED]>,
    iovecs: Vec<libc::iovec>,
    msgs: Vec<libc::mmsghdr>,
    addrs: Vec<libc::sockaddr_in>,
    ctrls: Vec<[u8; CTRL_LEN]>,
}

impl RecvArena {
    fn new() -> Self {
        let mut a = RecvArena {
            bufs: vec![[0u8; MAX_SHRED]; BATCH],
            iovecs: vec![unsafe { mem::zeroed() }; BATCH],
            msgs: vec![unsafe { mem::zeroed() }; BATCH],
            addrs: vec![unsafe { mem::zeroed() }; BATCH],
            ctrls: vec![[0u8; CTRL_LEN]; BATCH],
        };
        for i in 0..BATCH {
            a.iovecs[i] = libc::iovec {
                iov_base: a.bufs[i].as_mut_ptr() as *mut libc::c_void,
                iov_len: MAX_SHRED,
            };
            let hdr = &mut a.msgs[i].msg_hdr;
            hdr.msg_name = &mut a.addrs[i] as *mut _ as *mut libc::c_void;
            hdr.msg_iov = &mut a.iovecs[i] as *mut libc::iovec;
            hdr.msg_iovlen = 1;
            hdr.msg_control = a.ctrls[i].as_mut_ptr() as *mut libc::c_void;
        }
        a.reset();
        a
    }

    /// `recvmmsg` overwrites these with actual sizes; stale values would make the
    /// kernel think the control buffer is too short.
    fn reset(&mut self) {
        for m in &mut self.msgs {
            m.msg_hdr.msg_namelen = mem::size_of::<libc::sockaddr_in>() as libc::socklen_t;
            m.msg_hdr.msg_controllen = CTRL_LEN as _;
            m.msg_hdr.msg_flags = 0;
        }
    }
}

fn rx_loop(
    sock: OwnedFd,
    port: u16,
    registry: Arc<Registry>,
    netmon: Arc<NetMon>,
    tx: Sender<Vec<Packet>>,
    stats: Arc<RxStats>,
    exit: Arc<AtomicBool>,
) {
    let fd = sock.as_raw_fd();
    let mut arena = RecvArena::new();
    // Keeps the NetMon lock cold: only the first sighting of each source is reported.
    let mut reported_ips: AHashSet<(ProviderId, Ipv4Addr)> = AHashSet::new();
    let mut last_ovfl: u32 = 0;

    while !exit.load(Ordering::Relaxed) {
        arena.reset();
        // 100 ms so a quiet socket still notices `exit`; recreated since the kernel may
        // decrement it. MSG_WAITFORONE returns without waiting for a full batch.
        let mut timeout = libc::timespec {
            tv_sec: 0,
            tv_nsec: 100_000_000,
        };
        let n = unsafe {
            libc::recvmmsg(
                fd,
                arena.msgs.as_mut_ptr(),
                BATCH as libc::c_uint,
                libc::MSG_WAITFORONE,
                &mut timeout,
            )
        };
        if n <= 0 {
            let err = io::Error::last_os_error();
            if n < 0 && err.kind() != io::ErrorKind::Interrupted && err.raw_os_error() != Some(libc::EAGAIN) {
                eprintln!("rx-{port}: recvmmsg: {err}");
            }
            continue;
        }

        let mut batch: Vec<Packet> = Vec::with_capacity(n as usize);
        for i in 0..n as usize {
            let msg = &arena.msgs[i];
            let len = msg.msg_len as usize;
            let (ts_ns, rxq_ovfl) = parse_cmsgs(&msg.msg_hdr);

            // Cumulative per-socket counter; account the delta.
            if let Some(ovfl) = rxq_ovfl {
                let delta = ovfl.wrapping_sub(last_ovfl);
                if delta > 0 {
                    stats.kernel_dropped.fetch_add(delta as u64, Ordering::Relaxed);
                    last_ovfl = ovfl;
                }
            }

            if msg.msg_hdr.msg_flags & libc::MSG_TRUNC != 0 {
                stats.truncated.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            if len == 0 {
                continue;
            }
            // Never substitute a userspace clock read for a missing kernel stamp.
            let Some(rx_unix_ns) = ts_ns else {
                stats.no_timestamp.fetch_add(1, Ordering::Relaxed);
                continue;
            };

            stats.received.fetch_add(1, Ordering::Relaxed);
            let src_ip = Ipv4Addr::from(u32::from_be(arena.addrs[i].sin_addr.s_addr));
            let Some(provider) = registry.resolve(src_ip, port) else {
                stats.unmatched.fetch_add(1, Ordering::Relaxed);
                continue;
            };
            if reported_ips.insert((provider, src_ip)) {
                netmon.observe(provider, src_ip);
            }

            batch.push(Packet {
                provider,
                rx_unix_ns,
                data: arena.bufs[i][..len].to_vec(),
            });
        }

        if !batch.is_empty() && tx.try_send(batch).is_err() {
            stats.channel_full.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Returns `(SCM_TIMESTAMPNS in ns, SO_RXQ_OVFL drop count)`.
fn parse_cmsgs(hdr: &libc::msghdr) -> (Option<i64>, Option<u32>) {
    let (mut ts_ns, mut rxq_ovfl) = (None, None);
    unsafe {
        let mut cmsg = libc::CMSG_FIRSTHDR(hdr);
        while !cmsg.is_null() {
            if (*cmsg).cmsg_level == libc::SOL_SOCKET {
                match (*cmsg).cmsg_type {
                    SCM_TIMESTAMPNS => {
                        if let Some(ts) = cmsg_value::<libc::timespec>(cmsg) {
                            ts_ns = Some(ts.tv_sec * 1_000_000_000 + ts.tv_nsec);
                        }
                    }
                    SO_RXQ_OVFL => {
                        if let Some(v) = cmsg_value::<u32>(cmsg) {
                            rxq_ovfl = Some(v);
                        }
                    }
                    _ => {}
                }
            }
            cmsg = libc::CMSG_NXTHDR(hdr, cmsg);
        }
    }
    (ts_ns, rxq_ovfl)
}

/// Reads the payload only if the kernel delivered at least `size_of::<T>()` bytes.
unsafe fn cmsg_value<T>(cmsg: *const libc::cmsghdr) -> Option<T> {
    if (*cmsg).cmsg_len < libc::CMSG_LEN(mem::size_of::<T>() as u32) as usize {
        return None;
    }
    Some(ptr::read_unaligned(libc::CMSG_DATA(cmsg) as *const T))
}
