//! UDP receive path.
//!
//! One thread per bound port. Each thread uses `recvmmsg(2)` to pull up to
//! `BATCH` datagrams per syscall, and reads the kernel's `SCM_TIMESTAMPNS`
//! control message for each one.
//!
//! The timestamp is taken by the kernel when the driver hands the packet up,
//! *before* it sits in the socket receive queue. That is the whole point: a
//! userspace `clock_gettime()` after `recv()` would fold our own scheduling
//! delay into the measurement, and at 100 kpps that noise is larger than the
//! provider-to-provider differences we are trying to resolve.

use std::{
    io,
    mem,
    net::Ipv4Addr,
    os::fd::{AsRawFd, RawFd},
    ptr,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
};

use anyhow::{Context, Result};
use crossbeam_channel::Sender;

use crate::registry::{ProviderId, Registry};

/// Datagrams pulled per `recvmmsg` call.
const BATCH: usize = 64;
/// Largest shred we will accept. Solana shreds are 1203/1228 bytes.
const MAX_SHRED: usize = 1500;

/// `SO_TIMESTAMPNS` is 35 on every Linux ABI we target. `libc` exposes it on
/// most, but not all, targets — define it rather than depend on that.
const SO_TIMESTAMPNS: libc::c_int = 35;
const SCM_TIMESTAMPNS: libc::c_int = SO_TIMESTAMPNS;

/// `SO_RXQ_OVFL` makes the kernel attach, to every datagram, a running count of
/// the datagrams it dropped on this socket because the receive queue was full.
///
/// Without it those losses are *invisible*: a shred the kernel threw away never
/// reaches us, so it shows up as a shred the provider never sent — our own
/// backlog, silently rebilled to the provider as packet loss. That is the one
/// failure this tool must never have, so we count them and say so.
const SO_RXQ_OVFL: libc::c_int = 40;

pub struct Packet {
    pub provider: ProviderId,
    /// CLOCK_REALTIME nanoseconds since the unix epoch, stamped by the kernel.
    pub rx_unix_ns: i64,
    pub data: Vec<u8>,
}

#[derive(Default)]
pub struct RxStats {
    pub received: AtomicU64,
    pub unmatched: AtomicU64,
    pub no_timestamp: AtomicU64,
    pub channel_full: AtomicU64,
    /// Datagrams the *kernel* dropped because our socket queue was full, read
    /// from `SO_RXQ_OVFL`. These are our losses, not the provider's, and they
    /// would otherwise masquerade as shreds the provider failed to send.
    pub kernel_dropped: AtomicU64,
    /// Datagrams larger than any shred, truncated by the kernel to fit our
    /// buffer. Never parsed: a truncated shred fails verification and would be
    /// reported as a provider defect when in fact we cut it in half.
    pub truncated: AtomicU64,
}

/// Bind one UDP socket per port and spawn a receive thread for each.
pub fn spawn_receivers(
    bind_ip: Ipv4Addr,
    ports: &[u16],
    registry: Arc<Registry>,
    tx: Sender<Vec<Packet>>,
    stats: Arc<RxStats>,
    exit: Arc<AtomicBool>,
) -> Result<Vec<std::thread::JoinHandle<()>>> {
    let mut handles = Vec::with_capacity(ports.len());
    for &port in ports {
        let sock = bind_socket(bind_ip, port)
            .with_context(|| format!("binding {bind_ip}:{port}"))?;
        let registry = registry.clone();
        let tx = tx.clone();
        let stats = stats.clone();
        let exit = exit.clone();
        handles.push(
            std::thread::Builder::new()
                .name(format!("rx-{port}"))
                .spawn(move || rx_loop(sock, port, registry, tx, stats, exit))?,
        );
    }
    Ok(handles)
}

struct Socket(RawFd);
impl AsRawFd for Socket {
    fn as_raw_fd(&self) -> RawFd {
        self.0
    }
}
impl Drop for Socket {
    fn drop(&mut self) {
        unsafe { libc::close(self.0) };
    }
}

fn bind_socket(ip: Ipv4Addr, port: u16) -> Result<Socket> {
    unsafe {
        let fd = libc::socket(libc::AF_INET, libc::SOCK_DGRAM, 0);
        if fd < 0 {
            return Err(io::Error::last_os_error().into());
        }
        let sock = Socket(fd);

        let on: libc::c_int = 1;
        // Ask the kernel to attach a CLOCK_REALTIME timespec to every datagram.
        if libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            SO_TIMESTAMPNS,
            &on as *const _ as *const libc::c_void,
            mem::size_of::<libc::c_int>() as libc::socklen_t,
        ) < 0
        {
            return Err(anyhow::anyhow!(
                "setsockopt(SO_TIMESTAMPNS) failed: {}",
                io::Error::last_os_error()
            ));
        }

        // Count what the kernel drops on this socket, so our own backlog can
        // never be mistaken for provider packet loss.
        if libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            SO_RXQ_OVFL,
            &on as *const _ as *const libc::c_void,
            mem::size_of::<libc::c_int>() as libc::socklen_t,
        ) < 0
        {
            eprintln!(
                "warning: setsockopt(SO_RXQ_OVFL) failed on port {port}: {} — kernel receive \
                 drops cannot be counted on this socket, and will be indistinguishable from \
                 shreds a provider never sent",
                io::Error::last_os_error()
            );
        }

        // A big receive buffer is the difference between measuring the network
        // and measuring our own backlog.
        //
        // The kernel silently CLAMPS this to `net.core.rmem_max` and still
        // returns success, so asking is not the same as getting: on a stock box
        // rmem_max is 208 KiB and this 64 MiB request quietly becomes 208 KiB.
        // Read it back and say so, because the failure mode is invisible — a
        // burst overruns the small queue, the shreds vanish, and the provider
        // gets the blame.
        let want: libc::c_int = 64 * 1024 * 1024;
        if libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_RCVBUF,
            &want as *const _ as *const libc::c_void,
            mem::size_of::<libc::c_int>() as libc::socklen_t,
        ) < 0
        {
            eprintln!(
                "warning: setsockopt(SO_RCVBUF) failed on port {port}: {}",
                io::Error::last_os_error()
            );
        }

        let mut got: libc::c_int = 0;
        let mut len = mem::size_of::<libc::c_int>() as libc::socklen_t;
        if libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_RCVBUF,
            &mut got as *mut _ as *mut libc::c_void,
            &mut len,
        ) == 0
        {
            // Linux reports back double what it allotted (it reserves half for
            // bookkeeping), so compare against 2x the request.
            if (got as i64) < 2 * want as i64 {
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
        }

        let addr = libc::sockaddr_in {
            sin_family: libc::AF_INET as libc::sa_family_t,
            sin_port: port.to_be(),
            sin_addr: libc::in_addr {
                s_addr: u32::from_ne_bytes(ip.octets()),
            },
            sin_zero: [0; 8],
        };
        if libc::bind(
            fd,
            &addr as *const _ as *const libc::sockaddr,
            mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
        ) < 0
        {
            return Err(io::Error::last_os_error().into());
        }
        Ok(sock)
    }
}

/// Scratch buffers for one `recvmmsg` call. Allocated once per thread.
struct RecvArena {
    bufs: Vec<[u8; MAX_SHRED]>,
    iovecs: Vec<libc::iovec>,
    msgs: Vec<libc::mmsghdr>,
    addrs: Vec<libc::sockaddr_in>,
    ctrls: Vec<[u8; 64]>,
}

impl RecvArena {
    fn new() -> Box<Self> {
        let mut a = Box::new(RecvArena {
            bufs: vec![[0u8; MAX_SHRED]; BATCH],
            iovecs: vec![unsafe { mem::zeroed() }; BATCH],
            msgs: vec![unsafe { mem::zeroed() }; BATCH],
            addrs: vec![unsafe { mem::zeroed() }; BATCH],
            ctrls: vec![[0u8; 64]; BATCH],
        });
        for i in 0..BATCH {
            a.iovecs[i] = libc::iovec {
                iov_base: a.bufs[i].as_mut_ptr() as *mut libc::c_void,
                iov_len: MAX_SHRED,
            };
            let hdr = &mut a.msgs[i].msg_hdr;
            hdr.msg_name = &mut a.addrs[i] as *mut _ as *mut libc::c_void;
            hdr.msg_namelen = mem::size_of::<libc::sockaddr_in>() as libc::socklen_t;
            hdr.msg_iov = &mut a.iovecs[i] as *mut libc::iovec;
            hdr.msg_iovlen = 1;
            hdr.msg_control = a.ctrls[i].as_mut_ptr() as *mut libc::c_void;
            hdr.msg_controllen = 64;
        }
        a
    }

    /// `recvmmsg` overwrites `msg_controllen`/`msg_namelen` with the *actual*
    /// sizes; they must be reset before the next call or the kernel will refuse
    /// to write a control message into a buffer it thinks is short.
    fn reset(&mut self) {
        for i in 0..BATCH {
            let hdr = &mut self.msgs[i].msg_hdr;
            hdr.msg_namelen = mem::size_of::<libc::sockaddr_in>() as libc::socklen_t;
            hdr.msg_controllen = 64;
            hdr.msg_flags = 0;
        }
    }
}

fn rx_loop(
    sock: Socket,
    port: u16,
    registry: Arc<Registry>,
    tx: Sender<Vec<Packet>>,
    stats: Arc<RxStats>,
    exit: Arc<AtomicBool>,
) {
    let fd = sock.as_raw_fd();
    let mut arena = RecvArena::new();
    // Last value of the kernel's cumulative per-socket drop counter.
    let mut last_ovfl: u32 = 0;
    // 100 ms so a quiet socket still notices `exit`.
    let mut timeout = libc::timespec {
        tv_sec: 0,
        tv_nsec: 100_000_000,
    };

    while !exit.load(Ordering::Relaxed) {
        arena.reset();
        // MSG_WAITFORONE: return as soon as at least one datagram is available
        // instead of blocking until the whole 64-slot batch fills. Without it,
        // the kernel only checks the timeout *after* each datagram, so a socket
        // that receives 1..63 packets then briefly quiets would hold those
        // packets undelivered until the 64th arrives. The timeout is reset each
        // iteration in case a kernel decrements it in place.
        timeout.tv_sec = 0;
        timeout.tv_nsec = 100_000_000;
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
            let len = arena.msgs[i].msg_len as usize;
            let cmsgs = parse_cmsgs(&arena.msgs[i].msg_hdr);

            // The kernel's own drop counter is cumulative for the socket's life.
            // Take the delta so a restart of the counter cannot double-count.
            if let Some(ovfl) = cmsgs.rxq_ovfl {
                let delta = ovfl.wrapping_sub(last_ovfl);
                if delta > 0 {
                    stats.kernel_dropped.fetch_add(delta as u64, Ordering::Relaxed);
                    last_ovfl = ovfl;
                }
            }

            // MSG_TRUNC means the datagram was bigger than our buffer and the
            // kernel cut it. No shred is this large, so whatever this is, it is
            // not one raw shred — and the truncated remains would fail merkle and
            // be reported as a provider defect that we ourselves inflicted.
            // Refuse to guess: count it and drop it.
            if arena.msgs[i].msg_hdr.msg_flags & libc::MSG_TRUNC != 0 {
                stats.truncated.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            if len == 0 || len > MAX_SHRED {
                continue;
            }
            let src_ip = Ipv4Addr::from(u32::from_be(arena.addrs[i].sin_addr.s_addr));

            let Some(rx_unix_ns) = cmsgs.ts_ns else {
                // No timestamp means the measurement is worthless for this
                // packet. Count it and move on; never substitute a wall clock
                // read here, it would look like data but be a lie.
                stats.no_timestamp.fetch_add(1, Ordering::Relaxed);
                continue;
            };

            stats.received.fetch_add(1, Ordering::Relaxed);
            let Some(provider) = registry.resolve(src_ip, port) else {
                stats.unmatched.fetch_add(1, Ordering::Relaxed);
                continue;
            };

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

/// Control messages we care about, pulled out in one pass.
#[derive(Default)]
struct Cmsgs {
    /// `SCM_TIMESTAMPNS`: kernel CLOCK_REALTIME stamp, taken at driver handoff.
    ts_ns: Option<i64>,
    /// `SO_RXQ_OVFL`: datagrams the kernel has dropped on this socket so far.
    rxq_ovfl: Option<u32>,
}

fn parse_cmsgs(hdr: &libc::msghdr) -> Cmsgs {
    let mut out = Cmsgs::default();
    unsafe {
        let mut cmsg = libc::CMSG_FIRSTHDR(hdr);
        while !cmsg.is_null() {
            let level = (*cmsg).cmsg_level;
            let ctype = (*cmsg).cmsg_type;
            let len = (*cmsg).cmsg_len as usize;

            if level == libc::SOL_SOCKET && ctype == SCM_TIMESTAMPNS {
                // Check the length before reading. A kernel/ABI that delivers a
                // different timespec width would otherwise have us read past the
                // control buffer.
                if len >= libc::CMSG_LEN(mem::size_of::<libc::timespec>() as u32) as usize {
                    let mut ts: libc::timespec = mem::zeroed();
                    ptr::copy_nonoverlapping(
                        libc::CMSG_DATA(cmsg),
                        &mut ts as *mut _ as *mut u8,
                        mem::size_of::<libc::timespec>(),
                    );
                    out.ts_ns = Some(ts.tv_sec as i64 * 1_000_000_000 + ts.tv_nsec as i64);
                }
            } else if level == libc::SOL_SOCKET && ctype == SO_RXQ_OVFL {
                if len >= libc::CMSG_LEN(mem::size_of::<u32>() as u32) as usize {
                    let mut v: u32 = 0;
                    ptr::copy_nonoverlapping(
                        libc::CMSG_DATA(cmsg),
                        &mut v as *mut _ as *mut u8,
                        mem::size_of::<u32>(),
                    );
                    out.rxq_ovfl = Some(v);
                }
            }
            cmsg = libc::CMSG_NXTHDR(hdr, cmsg);
        }
    }
    out
}
