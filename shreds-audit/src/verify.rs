//! Per-shred parse and signature verification. Each shred's merkle root is
//! recomputed from its own proof; the ed25519 check runs once per distinct
//! `(sig, root)` in a chunk, since every shred of a FEC set shares both.

use ahash::AHashMap;
use ed25519_dalek::{Signature as DalekSig, VerifyingKey};
use rayon::prelude::*;
use serde::Serialize;
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
const SIZE_OF_SIGNATURE: usize = 64;
const LAST_IN_SLOT_FLAGS: u8 = 0b1100_0000;
const DATA_COMPLETE_FLAG: u8 = 0b0100_0000;

type Triple = ([u8; 64], [u8; 32], Pubkey);

pub struct VerifiedShred {
    pub packet_index: usize,
    pub provider: ProviderId,
    pub rx_unix_ns: i64,
    pub slot: u64,
    pub fec_set_index: u32,
    pub shred_index: u32,
    pub is_code: bool,
    pub position: u32,
    pub last_in_slot: bool,
    /// Last data shred of its FEC set, so `position + 1` is the set's data-shred count.
    pub data_complete: bool,
    pub num_data: Option<u16>,
    pub num_coding: Option<u16>,
    pub leader: Option<Pubkey>,
    /// `None` when the slot's leader is unknown: no verdict, not a failure.
    pub sig_ok: Option<bool>,
    pub merkle_ok: bool,
    pub proof_stripped: bool,
    /// FNV-1a of the full payload, for duplicate detection.
    pub payload_hash: u64,
    /// SHA-256 of the block data only (see `data_range`), so a broken proof can be
    /// told apart from altered content.
    pub data_hash: Option<[u8; 32]>,
}

impl VerifiedShred {
    pub fn is_authentic(&self) -> bool {
        self.merkle_ok && self.sig_ok != Some(false)
    }
}

#[derive(Default, Clone, Copy, Serialize)]
pub struct ProviderVerifyStats {
    pub parsed: u64,
    pub malformed: u64,
    pub unsupported_variant: u64,
    pub wrong_version: u64,
    pub no_merkle_root: u64,
    pub no_leader: u64,
    pub sig_bad: u64,
    pub proof_stripped: u64,
}

#[derive(Default, Clone)]
pub struct VerifyStats {
    pub parsed: u64,
    pub malformed: u64,
    pub unsupported_variant: u64,
    pub non_shred_ping: u64,
    pub wrong_version: u64,
    pub no_merkle_root: u64,
    pub no_leader: u64,
    pub sig_bad: u64,
    pub proof_stripped: u64,
    pub ed25519_verifies: u64,
    pub batch_fallbacks: u64,
    pub providers: Vec<ProviderVerifyStats>,
}

impl VerifyStats {
    fn provider(&mut self, provider: ProviderId) -> &mut ProviderVerifyStats {
        let index = provider as usize;
        if index >= self.providers.len() {
            self.providers.resize(index + 1, ProviderVerifyStats::default());
        }
        &mut self.providers[index]
    }
}

pub fn verify_chunk(
    packets: &[Packet],
    schedule: &LeaderSchedule,
    shred_version: Option<u16>,
    stats: &mut VerifyStats,
) -> Vec<VerifiedShred> {
    // Each shred with the index of its (sig, root) triple, if a verdict is possible.
    let mut pending: Vec<(VerifiedShred, Option<usize>)> = Vec::with_capacity(packets.len());
    let mut dedup: AHashMap<([u8; 64], [u8; 32]), usize> = AHashMap::new();
    let mut triples: Vec<Triple> = Vec::new();

    for (packet_index, p) in packets.iter().enumerate() {
        let s: &[u8] = &p.data;
        if s.len() < CODING_HEADER_LEN + 1 {
            stats.malformed += 1;
            stats.provider(p.provider).malformed += 1;
            continue;
        }
        // A relayed ping is a valid protocol message, not a provider defect.
        if is_ping(s) {
            stats.non_shred_ping += 1;
            continue;
        }
        if shred_version.is_some_and(|want| read_u16(s, VERSION_OFFSET) != want) {
            stats.wrong_version += 1;
            stats.provider(p.provider).wrong_version += 1;
            continue;
        }
        // An unknown variant can't have its merkle root rebuilt; it must not count as invalid.
        let Some((is_code, _, _, _)) = decode_variant(s[VARIANT_OFFSET]) else {
            stats.unsupported_variant += 1;
            stats.provider(p.provider).unsupported_variant += 1;
            continue;
        };
        let (Some(slot), Some(fec_set_index), Some(shred_index)) = (
            layout::get_slot(s),
            layout::get_fec_set_index(s),
            layout::get_index(s),
        ) else {
            stats.malformed += 1;
            stats.provider(p.provider).malformed += 1;
            continue;
        };

        let leader = match schedule.classify(slot) {
            SlotVerdict::Implausible => {
                stats.malformed += 1;
                stats.provider(p.provider).malformed += 1;
                continue;
            }
            SlotVerdict::Leader(pk) => Some(pk),
            SlotVerdict::Unknown => {
                stats.no_leader += 1;
                stats.provider(p.provider).no_leader += 1;
                None
            }
        };
        stats.parsed += 1;
        stats.provider(p.provider).parsed += 1;

        let (num_data, num_coding, position) = if is_code {
            (
                Some(read_u16(s, CODING_NUM_DATA_OFFSET)),
                Some(read_u16(s, CODING_NUM_CODING_OFFSET)),
                read_u16(s, CODING_POSITION_OFFSET) as u32,
            )
        } else {
            (None, None, shred_index.saturating_sub(fec_set_index))
        };
        let flags = s[DATA_FLAGS_OFFSET];
        let last_in_slot = !is_code && flags & LAST_IN_SLOT_FLAGS == LAST_IN_SLOT_FLAGS;
        let data_complete = !is_code && flags & DATA_COMPLETE_FLAG != 0;

        let root = layout::get_merkle_root(s);
        let merkle_ok = root.is_some();
        if !merkle_ok {
            stats.no_merkle_root += 1;
            stats.provider(p.provider).no_merkle_root += 1;
        }

        let sig: [u8; 64] = s[..64].try_into().unwrap();
        let key = root.zip(leader).map(|(root, leader)| {
            let root = root.to_bytes();
            *dedup.entry((sig, root)).or_insert_with(|| {
                triples.push((sig, root, leader));
                triples.len() - 1
            })
        });

        pending.push((
            VerifiedShred {
                packet_index,
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
                proof_stripped: proof_range(s).is_some_and(|range| s[range].iter().all(|&b| b == 0)),
                payload_hash: fnv1a(s),
                data_hash: data_range(s).map(|r| solana_sdk::hash::hash(&s[r]).to_bytes()),
            },
            key,
        ));
    }

    let verdicts = verify_triples(&triples, stats);

    pending
        .into_iter()
        .map(|(mut shred, key)| {
            shred.sig_ok = key.map(|i| verdicts[i]);
            if shred.sig_ok == Some(false) {
                if shred.proof_stripped {
                    stats.proof_stripped += 1;
                    stats.provider(shred.provider).proof_stripped += 1;
                } else {
                    stats.sig_bad += 1;
                    stats.provider(shred.provider).sig_bad += 1;
                }
            }
            shred
        })
        .collect()
}

fn verify_triples(triples: &[Triple], stats: &mut VerifyStats) -> Vec<bool> {
    stats.ed25519_verifies += triples.len() as u64;

    // Below this, batch setup costs more than verifying one by one.
    const BATCH_MIN: usize = 8;
    if triples.len() < BATCH_MIN {
        return triples.iter().map(verify_one).collect();
    }

    let mut msgs: Vec<&[u8]> = Vec::with_capacity(triples.len());
    let mut sigs: Vec<DalekSig> = Vec::with_capacity(triples.len());
    let mut keys: Vec<VerifyingKey> = Vec::with_capacity(triples.len());
    for (sig, root, leader) in triples {
        let Ok(vk) = VerifyingKey::from_bytes(&leader.to_bytes()) else {
            return triples.iter().map(verify_one).collect();
        };
        msgs.push(root.as_slice());
        sigs.push(DalekSig::from_bytes(sig));
        keys.push(vk);
    }

    if ed25519_dalek::verify_batch(&msgs, &sigs, &keys).is_ok() {
        return vec![true; triples.len()];
    }
    // A failed batch can't say which signature is bad; invalid shreds are routine, so this is hot.
    stats.batch_fallbacks += 1;
    triples.par_iter().map(verify_one).collect()
}

fn verify_one((sig, root, leader): &Triple) -> bool {
    VerifyingKey::from_bytes(&leader.to_bytes())
        .is_ok_and(|vk| vk.verify_strict(root, &DalekSig::from_bytes(sig)).is_ok())
}

fn read_u16(s: &[u8], off: usize) -> u16 {
    u16::from_le_bytes([s[off], s[off + 1]])
}

/// `(is_code, proof_size, chained, resigned)` from agave's `ShredVariant` byte.
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

/// The shred's block data: `[64..D)`, after the leader signature and before the
/// authentication tail (chained root, merkle proof, retransmitter signature),
/// where `D = SIZE_OF_PAYLOAD - 32*chained - 20*proof_size - 64*resigned`.
/// Deliberately not agave's merkle leaf, which also covers the chained root: a
/// relay that wrecks the tail has broken authentication, not altered block data.
fn data_range(s: &[u8]) -> Option<std::ops::Range<usize>> {
    let proof = proof_range(s)?;
    (proof.start > SIZE_OF_SIGNATURE).then_some(SIZE_OF_SIGNATURE..proof.start)
}

pub(crate) fn proof_range(s: &[u8]) -> Option<std::ops::Range<usize>> {
    const SIZE_OF_DATA_PAYLOAD: usize = 1203;
    const SIZE_OF_CODE_PAYLOAD: usize = 1228;
    const SIZE_OF_MERKLE_ROOT: usize = 32;
    const SIZE_OF_PROOF_ENTRY: usize = 20;

    let (is_code, proof_size, chained, resigned) = decode_variant(*s.get(VARIANT_OFFSET)?)?;
    let payload = if is_code { SIZE_OF_CODE_PAYLOAD } else { SIZE_OF_DATA_PAYLOAD };
    let proof_end = payload.checked_sub(SIZE_OF_SIGNATURE * usize::from(resigned))?;
    let proof_start = proof_end
        .checked_sub(SIZE_OF_MERKLE_ROOT * usize::from(chained) + SIZE_OF_PROOF_ENTRY * proof_size)?;
    (proof_start > SIZE_OF_SIGNATURE && proof_start < proof_end && s.len() >= proof_end)
        .then_some(proof_start..proof_end)
}

/// A Solana ping (`u32 = 4`, pubkey, 32-byte token, signature over the token).
/// Checked before the shred parse: byte 64 lands in the random token, so about half
/// of pings would otherwise parse as shreds at nonsense slots. The signature must
/// verify, so only a genuine ping is excused from `malformed`.
fn is_ping(s: &[u8]) -> bool {
    const PING_LEN: usize = 132;
    const PING_DISCRIMINANT: u32 = 4;

    if s.len() != PING_LEN || u32::from_le_bytes([s[0], s[1], s[2], s[3]]) != PING_DISCRIMINANT {
        return false;
    }
    let from: [u8; 32] = s[4..36].try_into().unwrap();
    let sig: [u8; 64] = s[68..132].try_into().unwrap();
    VerifyingKey::from_bytes(&from)
        .is_ok_and(|vk| vk.verify_strict(&s[36..68], &DalekSig::from_bytes(&sig)).is_ok())
}

fn fnv1a(s: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in s {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}
