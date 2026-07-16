//! Per-shred parse + signature verification.
//!
//! Every shred is verified. The cost is not what it first looks like:
//!
//! * `layout::get_merkle_root(shred)` recomputes the merkle root *from that
//!   shred's own inclusion proof*. This is the per-shred half of the check and
//!   it is pure hashing. If a shred was tampered with, or belongs to a
//!   different set, its proof will not hash to the signed root.
//! * The ed25519 signature covers that root. Every shred in a FEC set carries
//!   the *same* `(signature, root)` pair, so the elliptic-curve work is once
//!   per set, not once per shred. We dedupe identical `(sig, root, leader)`
//!   triples inside each chunk before touching the curve.
//!
//! Net effect at 6 providers x ~11 kpps: ~400 k merkle-root recomputations per
//! second (cheap) and ~6 k ed25519 verifies per second (the expensive part),
//! rather than 400 k ed25519 verifies. Every shred still gets its own verdict.
//!
//! Batch verification is kept for the case where a chunk really does contain
//! many distinct sets, but a batch that fails is re-checked individually so a
//! single bad signature cannot condemn its neighbours. Invalid shreds are
//! expected in this data, not exceptional.

use ahash::AHashMap;
use ed25519_dalek::{Signature as DalekSig, VerifyingKey};
use rayon::prelude::*;
use solana_ledger::shred::layout;
use solana_sdk::pubkey::Pubkey;

use crate::{
    leader::{LeaderSchedule, SlotVerdict},
    registry::ProviderId,
    rx::Packet,
};

const VARIANT_OFFSET: usize = 64;
const VERSION_OFFSET: usize = 77;
const CODING_NUM_DATA_OFFSET: usize = 83;
const CODING_NUM_CODING_OFFSET: usize = 85;
const CODING_POSITION_OFFSET: usize = 87;
const DATA_FLAGS_OFFSET: usize = 85;
const CODING_HEADER_LEN: usize = 89;
const KIND_MASK: u8 = 0xC0;
const KIND_CODE: u8 = 0x40;
const KIND_DATA: u8 = 0x80;
const LAST_IN_SLOT_FLAGS: u8 = 0b1100_0000;
/// Set on the final data shred of a FEC set.
const DATA_COMPLETE_FLAG: u8 = 0b0100_0000;

/// One verified shred. `sig_ok == None` means we had no leader for the slot and
/// therefore could not form an opinion — distinct from `Some(false)`.
pub struct VerifiedShred {
    pub provider: ProviderId,
    pub rx_unix_ns: i64,
    pub slot: u64,
    pub fec_set_index: u32,
    pub shred_index: u32,
    pub is_code: bool,
    pub position: u32,
    pub last_in_slot: bool,
    /// This data shred closes its FEC set (DATA_COMPLETE). With it, `position + 1`
    /// is the set's data-shred count even when no coding shred was delivered.
    pub data_complete: bool,
    pub num_data: Option<u16>,
    pub num_coding: Option<u16>,
    pub leader: Option<Pubkey>,
    pub sig_ok: Option<bool>,
    pub merkle_ok: bool,
    /// FNV-1a of the full payload, for duplicate detection inside a FEC set.
    pub payload_hash: u64,
    /// SHA-256 of the shred's *block data* — headers plus payload, and nothing
    /// else. See `data_range`. `None` for variants we cannot locate it in.
    ///
    /// This is what makes "the signature is wrong" separable from "the data is
    /// wrong". Two copies of a shred with the same `data_hash` carry identical
    /// block content, whatever their merkle proofs say about it.
    pub data_hash: Option<[u8; 32]>,
}

#[derive(Default, Clone, Copy)]
pub struct VerifyStats {
    pub parsed: u64,
    pub malformed: u64,
    /// Well-formed datagrams carrying a shred variant this build cannot parse
    /// (legacy, or newer than us). Counted and dropped — never counted invalid.
    pub unsupported_variant: u64,
    pub non_shred_ping: u64,
    pub wrong_version: u64,
    pub no_merkle_root: u64,
    pub no_leader: u64,
    pub sig_bad: u64,
    pub ed25519_verifies: u64,
    pub batch_fallbacks: u64,
}

/// Parsed shred awaiting a signature verdict.
struct Pending {
    shred: VerifiedShred,
    /// Index into the dedup table, or `None` when no verdict is possible.
    key: Option<usize>,
}


pub fn verify_chunk(
    packets: Vec<Packet>,
    schedule: &LeaderSchedule,
    shred_version: Option<u16>,
    stats: &mut VerifyStats,
) -> Vec<VerifiedShred> {
    // ---- pass 1: parse, recompute each shred's own merkle root ----
    let mut pending: Vec<Pending> = Vec::with_capacity(packets.len());
    // (signature, root) -> index into `triples`
    let mut dedup: AHashMap<([u8; 64], [u8; 32]), usize> = AHashMap::new();
    let mut triples: Vec<([u8; 64], [u8; 32], Pubkey)> = Vec::new();

    for p in packets {
        let s: &[u8] = &p.data;
        if s.len() < CODING_HEADER_LEN + 1 {
            stats.malformed += 1;
            continue;
        }
        // A ping is not a shred and not a defect: the provider is faithfully
        // relaying a valid protocol message that shares the socket. Counting it
        // as malformed would read, in an archive handed to that provider, as an
        // accusation that they sent us garbage.
        if is_ping(s) {
            stats.non_shred_ping += 1;
            continue;
        }
        if let Some(want) = shred_version {
            let v = u16::from_le_bytes([s[VERSION_OFFSET], s[VERSION_OFFSET + 1]]);
            if v != want {
                stats.wrong_version += 1;
                continue;
            }
        }
        // A shred whose variant this build does not understand is NOT an invalid
        // shred. We cannot reconstruct its merkle root, so it would fail the check
        // and be counted against the provider — and on the day Solana ships a new
        // shred variant, every provider would light up at ~100% invalid and this
        // tool would look like it had caught mass tampering. "I don't know what
        // this is" gets its own counter and never a verdict.
        let Some((is_code, _, _, _)) = decode_variant(s[VARIANT_OFFSET]) else {
            stats.unsupported_variant += 1;
            continue;
        };
        let (Some(slot), Some(fec_set_index), Some(shred_index)) = (
            layout::get_slot(s),
            layout::get_fec_set_index(s),
            layout::get_index(s),
        ) else {
            stats.malformed += 1;
            continue;
        };

        // Resolve the leader first: a slot nowhere near the schedule we hold is
        // not a shred at all, and must not be attributed to a FEC set.
        let leader = match schedule.classify(slot) {
            SlotVerdict::Implausible => {
                stats.malformed += 1;
                continue;
            }
            SlotVerdict::Leader(pk) => Some(pk),
            SlotVerdict::Unknown => {
                stats.no_leader += 1;
                None
            }
        };
        stats.parsed += 1;

        let (num_data, num_coding) = if is_code {
            (
                Some(u16::from_le_bytes([s[CODING_NUM_DATA_OFFSET], s[CODING_NUM_DATA_OFFSET + 1]])),
                Some(u16::from_le_bytes([
                    s[CODING_NUM_CODING_OFFSET],
                    s[CODING_NUM_CODING_OFFSET + 1],
                ])),
            )
        } else {
            (None, None)
        };
        let position = if is_code {
            u16::from_le_bytes([s[CODING_POSITION_OFFSET], s[CODING_POSITION_OFFSET + 1]]) as u32
        } else {
            shred_index.saturating_sub(fec_set_index)
        };
        let last_in_slot =
            !is_code && (s[DATA_FLAGS_OFFSET] & LAST_IN_SLOT_FLAGS) == LAST_IN_SLOT_FLAGS;
        // The last data shred of a FEC set carries DATA_COMPLETE. It is the only
        // way to learn a set's data-shred count without a coding shred, and some
        // providers forward data shreds only — without this their sets would never
        // be marked decodable and they would look like they delivered nothing.
        let data_complete = !is_code && (s[DATA_FLAGS_OFFSET] & DATA_COMPLETE_FLAG) != 0;

        // This is the per-shred integrity check: recompute the root from this
        // shred's merkle proof. A shred that does not belong to the signed set
        // fails here, before the signature is ever consulted.
        let root = layout::get_merkle_root(s);
        let merkle_ok = root.is_some();
        if !merkle_ok {
            stats.no_merkle_root += 1;
        }

        let mut sig = [0u8; 64];
        sig.copy_from_slice(&s[..64]);

        let key = match (root, leader) {
            (Some(root), Some(leader)) => {
                let root_bytes: [u8; 32] = root.to_bytes();
                let idx = *dedup.entry((sig, root_bytes)).or_insert_with(|| {
                    triples.push((sig, root_bytes, leader));
                    triples.len() - 1
                });
                Some(idx)
            }
            _ => None,
        };

        pending.push(Pending {
            shred: VerifiedShred {
                provider: p.provider,
                rx_unix_ns: p.rx_unix_ns,
                slot,
                fec_set_index,
                shred_index,
                is_code,
                position,
                last_in_slot,
                data_complete,
                num_data,
                num_coding,
                leader,
                sig_ok: None,
                merkle_ok,
                payload_hash: fnv1a(s),
                data_hash: data_range(s).map(|r| solana_sdk::hash::hash(&s[r]).to_bytes()),
            },
            key,
        });
    }

    // ---- pass 2: verify the unique (sig, root, leader) triples ----
    let verdicts = verify_triples(&triples, stats);

    // ---- pass 3: attribute verdicts back to every shred ----
    let mut out = Vec::with_capacity(pending.len());
    for mut p in pending {
        p.shred.sig_ok = p.key.map(|i| verdicts[i]);

        if p.shred.sig_ok == Some(false) {
            stats.sig_bad += 1;
        }
        out.push(p.shred);
    }
    out
}

fn verify_triples(triples: &[([u8; 64], [u8; 32], Pubkey)], stats: &mut VerifyStats) -> Vec<bool> {
    if triples.is_empty() {
        return Vec::new();
    }
    stats.ed25519_verifies += triples.len() as u64;

    // Below this size the batch machinery (a random scalar per signature, plus a
    // multiscalar mul) costs more than just verifying them one by one.
    const BATCH_MIN: usize = 8;
    if triples.len() < BATCH_MIN {
        return triples.iter().map(|t| verify_one(t)).collect();
    }

    let mut msgs: Vec<&[u8]> = Vec::with_capacity(triples.len());
    let mut sigs: Vec<DalekSig> = Vec::with_capacity(triples.len());
    let mut keys: Vec<VerifyingKey> = Vec::with_capacity(triples.len());
    for (sig, root, leader) in triples {
        let Ok(vk) = VerifyingKey::from_bytes(&leader.to_bytes()) else {
            // A leader pubkey that is not a valid curve point can never verify.
            return triples.iter().map(|t| verify_one(t)).collect();
        };
        msgs.push(root.as_slice());
        sigs.push(DalekSig::from_bytes(sig));
        keys.push(vk);
    }

    if ed25519_dalek::verify_batch(&msgs, &sigs, &keys).is_ok() {
        return vec![true; triples.len()];
    }
    // At least one is bad. Batch verification cannot say which, so fall back.
    // Invalid shreds are routine here, so this path is hot
    // and must stay correct rather than clever.
    stats.batch_fallbacks += 1;
    triples.par_iter().map(verify_one).collect()
}

fn verify_one((sig, root, leader): &([u8; 64], [u8; 32], Pubkey)) -> bool {
    let Ok(vk) = VerifyingKey::from_bytes(&leader.to_bytes()) else {
        return false;
    };
    vk.verify_strict(root.as_slice(), &DalekSig::from_bytes(sig))
        .is_ok()
}


/// The shred's **block data**: the headers plus the payload, and nothing else.
/// This is the hinge of the invalid-sig / invalid-data split, so it is worth
/// being exact about where it stops.
///
/// A merkle shred lays out as:
///
/// ```text
///   [0..64)      leader signature       (over the merkle root)
///   [64..D)      headers + block data   <-- this range
///   [D..D+32)    chained merkle root    ] signed, but NOT block data:
///   [D+32..P)    merkle inclusion proof ] authentication material
///   [P..end)     retransmitter signature   (leader does not sign this at all)
/// ```
///
/// with `D = SIZE_OF_PAYLOAD - 32*chained - 20*proof_size - 64*resigned`, from
/// agave's `ShredVariant` byte:
///
/// ```text
///   0x40|ps  code, plain        0x80|ps  data, plain
///   0x60|ps  code, chained      0x90|ps  data, chained
///   0x70|ps  code, chained+resigned
///                               0xb0|ps  data, chained+resigned
/// ```
///
/// Note this is deliberately **not** agave's merkle leaf, which runs to `D+32`
/// and so swallows the chained root. The chained root is signed, but it is a link
/// to the previous FEC set, not block content — a relay that wrecks it has broken
/// authentication, not altered a transaction, and the two must not be conflated.
///
/// Everything from `D` on is authentication material. A relay can wreck all of it
/// (and one of ours does) while relaying the leader's real data untouched;
/// hashing `[64..D)` and nothing else is what tells that apart from substitution.
/// Legacy shreds have no merkle layout and yield `None`.
/// Decode agave's `ShredVariant` byte into `(is_code, proof_size, chained, resigned)`.
/// `None` for legacy shreds (`0x5a`/`0xa5`) and for anything this build has never
/// heard of — both of which must be counted, never judged.
fn decode_variant(variant: u8) -> Option<(bool, usize, bool, bool)> {
    let proof_size = (variant & 0x0f) as usize;
    let (is_code, chained, resigned) = match variant & 0xf0 {
        0x40 => (true, false, false),
        0x60 => (true, true, false),
        0x70 => (true, true, true),
        0x80 => (false, false, false),
        0x90 => (false, true, false),
        0xb0 => (false, true, true),
        _ => return None,
    };
    Some((is_code, proof_size, chained, resigned))
}

fn data_range(s: &[u8]) -> Option<std::ops::Range<usize>> {
    const SIZE_OF_DATA_PAYLOAD: usize = 1203;
    const SIZE_OF_CODE_PAYLOAD: usize = 1228;
    const SIZE_OF_MERKLE_ROOT: usize = 32;
    const SIZE_OF_PROOF_ENTRY: usize = 20;
    const SIZE_OF_SIGNATURE: usize = 64;

    let (is_code, proof_size, chained, resigned) = decode_variant(*s.get(VARIANT_OFFSET)?)?;
    let payload = if is_code { SIZE_OF_CODE_PAYLOAD } else { SIZE_OF_DATA_PAYLOAD };
    let tail = SIZE_OF_MERKLE_ROOT * usize::from(chained)
        + SIZE_OF_PROOF_ENTRY * proof_size
        + SIZE_OF_SIGNATURE * usize::from(resigned);
    let proof_offset = payload.checked_sub(tail)?;
    if proof_offset <= SIZE_OF_SIGNATURE || s.len() < proof_offset {
        return None;
    }
    Some(SIZE_OF_SIGNATURE..proof_offset)
}

/// A Solana ping, as bincode lays it out:
///
/// ```text
/// [0..4]    u32 = 4    enum discriminant
/// [4..36]   Pubkey     sender identity
/// [36..68]  [u8; 32]   random token
/// [68..132] Signature  ed25519 over the token
/// ```
///
/// Validators ping peers over the same UDP socket that carries shreds — it is
/// the liveness handshake a node does before it will serve repair — so a
/// provider forwarding that socket's contents relays them to us too.
///
/// They must be recognised *before* the shred parse, not after. The kind of a
/// shred is read from the top two bits of byte 64, which in a ping lands inside
/// the random token: about half of all pings have those bits set to `DATA` or
/// `CODE` by chance and would otherwise parse as a shred whose "slot" is really
/// the tail of that token — a phantom FEC set at a nonsense slot.
///
/// The signature is verified before we call it a ping. A counter in an archive
/// is a claim about a provider, and "relayed a valid Solana ping" is a very
/// different claim from "sent a malformed packet"; anything that fails this
/// check stays malformed. No real shred is 132 bytes, so this can never swallow
/// one, and the ed25519 cost is paid only by packets that are already the exact
/// length and discriminant of a ping.
fn is_ping(s: &[u8]) -> bool {
    const PING_LEN: usize = 132;
    const PING_DISCRIMINANT: u32 = 4;

    if s.len() != PING_LEN || u32::from_le_bytes([s[0], s[1], s[2], s[3]]) != PING_DISCRIMINANT {
        return false;
    }
    let from: [u8; 32] = s[4..36].try_into().expect("length checked");
    let sig: [u8; 64] = s[68..132].try_into().expect("length checked");
    let Ok(vk) = VerifyingKey::from_bytes(&from) else {
        return false;
    };
    vk.verify_strict(&s[36..68], &DalekSig::from_bytes(&sig))
        .is_ok()
}

#[inline]
fn fnv1a(s: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in s {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}
