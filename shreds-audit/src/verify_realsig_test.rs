//! Signature-verification tests against real leader-signed merkle shreds.

#![cfg(test)]

use solana_entry::entry::Entry;
use solana_ledger::shred::{ProcessShredsStats, ReedSolomonCache, Shredder};
use solana_sdk::{hash::Hash, signature::Keypair, signer::Signer};

use crate::{
    leader::LeaderSchedule,
    rx::Packet,
    verify::{verify_chunk, VerifyStats},
};

fn make_real_shreds(slot: u64, leader: &Keypair) -> Vec<Vec<u8>> {
    let shredder = Shredder::new(slot, slot.saturating_sub(1), 0, /*version*/ 42).unwrap();
    let cache = ReedSolomonCache::default();
    let mut stats = ProcessShredsStats::default();
    let entries: Vec<Entry> = (0..2)
        .map(|_| Entry::new(&Hash::default(), 1, vec![]))
        .collect();
    let (data, coding) = shredder.entries_to_merkle_shreds_for_tests(
        leader,
        &entries,
        /*is_last_in_slot*/ true,
        Hash::default(),
        /*next_shred_index*/ 0,
        /*next_code_index*/ 0,
        &cache,
        &mut stats,
    );
    data.iter()
        .chain(coding.iter())
        .map(|s| s.payload().to_vec())
        .collect()
}

fn packets(shreds: &[Vec<u8>]) -> Vec<Packet> {
    shreds
        .iter()
        .enumerate()
        .map(|(i, s)| Packet {
            provider: 0,
            rx_unix_ns: 1_000_000_000 + i as i64,
            data: s.clone(),
        })
        .collect()
}

#[test]
fn counts_rejects_per_provider_and_marks_forgeries_not_authentic() {
    let leader = Keypair::new();
    let slot = 1000;
    let shreds = make_real_shreds(slot, &leader);
    let sched = LeaderSchedule::for_test(slot, vec![Some(leader.pubkey())]);
    let mut forged = shreds[0].clone();
    forged[100] ^= 0xff;
    let input = vec![
        Packet { provider: 0, rx_unix_ns: 1, data: shreds[0].clone() },
        Packet { provider: 1, rx_unix_ns: 2, data: forged },
        Packet { provider: 1, rx_unix_ns: 3, data: vec![0; 10] },
    ];

    let mut stats = VerifyStats::default();
    let out = verify_chunk(&input, &sched, None, &mut stats);

    let authentic: Vec<_> = out.iter().map(|s| (s.packet_index, s.is_authentic())).collect();
    assert_eq!(authentic, [(0, true), (1, false)]);
    let counts = |p: &crate::verify::ProviderVerifyStats| (p.parsed, p.sig_bad, p.malformed);
    assert_eq!(counts(&stats.providers[0]), (1, 0, 0));
    assert_eq!(counts(&stats.providers[1]), (1, 1, 1));
}

#[test]
fn a_zeroed_merkle_proof_is_proof_stripped_not_a_bad_signature() {
    let leader = Keypair::new();
    let slot = 1000;
    let shreds = make_real_shreds(slot, &leader);
    let sched = LeaderSchedule::for_test(slot, vec![Some(leader.pubkey())]);
    let mut stripped = shreds[0].clone();
    let range = crate::verify::proof_range(&stripped).unwrap();
    stripped[range].fill(0);
    let mut forged = shreds[1].clone();
    forged[100] ^= 0xff;

    let mut stats = VerifyStats::default();
    let out = verify_chunk(&packets(&[stripped, forged]), &sched, None, &mut stats);

    assert!(out.iter().all(|s| s.sig_ok == Some(false)));
    assert_eq!((stats.proof_stripped, stats.sig_bad), (1, 1));
    assert_eq!(stats.providers[0].proof_stripped, 1);
}

/// `token[28]` is datagram byte 64, where a shred's kind is read from.
fn make_ping(sender: &Keypair, kind_bits: u8) -> Vec<u8> {
    let mut token = [7u8; 32];
    token[28] = kind_bits;

    let mut p = Vec::with_capacity(132);
    p.extend_from_slice(&4u32.to_le_bytes());
    p.extend_from_slice(sender.pubkey().as_ref());
    p.extend_from_slice(&token);
    p.extend_from_slice(sender.sign_message(&token).as_ref());
    assert_eq!(p.len(), 132);
    p
}

#[test]
fn ping_is_not_a_shred_and_not_a_defect() {
    let sender = Keypair::new();
    let pings = vec![make_ping(&sender, 0x80), make_ping(&sender, 0x40)];

    let sched = LeaderSchedule::for_test(1000, vec![Some(Keypair::new().pubkey())]);
    let mut stats = VerifyStats::default();
    let out = verify_chunk(&packets(&pings), &sched, None, &mut stats);

    assert!(out.is_empty(), "a ping must never reach the aggregator as a shred");
    assert_eq!(stats.non_shred_ping, 2, "both pings should be recognised");
    assert_eq!(stats.parsed, 0);
    assert_eq!(stats.sig_bad, 0, "a ping must never be reported as a bad signature");
    assert_eq!(
        stats.malformed, 0,
        "a valid relayed ping is not a provider defect and must not be counted as malformed"
    );
}

/// Flips every byte of a real shred and checks that `data_hash` covers exactly
/// the signed block data, and that the rest is the variant's declared auth tail.
#[test]
fn data_hash_covers_block_content_and_only_block_content() {
    let leader = Keypair::new();
    let slot = 1000;
    let shreds = make_real_shreds(slot, &leader);
    let sched = LeaderSchedule::for_test(slot, vec![Some(leader.pubkey())]);

    // No `shred_version` filter: flipped version bytes would just be dropped.
    let mut stats = VerifyStats::default();
    let base = verify_chunk(&packets(&shreds), &sched, None, &mut stats);
    assert!(base.iter().all(|s| s.sig_ok == Some(true)));

    let i = base.iter().position(|s| !s.is_code).expect("a data shred");
    let good = base[i].data_hash.expect("a real shred must yield a data hash");
    let payload = shreds[i].clone();

    let (mut moved, mut rejected) = (Vec::new(), Vec::new());
    for off in 0..payload.len() {
        let mut p = payload.clone();
        p[off] ^= 0xff;
        let mut st = VerifyStats::default();
        let out = verify_chunk(&packets(&[p]), &sched, None, &mut st);

        // A dropped shred (e.g. corrupted slot) counts as both moved and rejected.
        let (hash, verified) = match out.first() {
            Some(v) => (v.data_hash, v.merkle_ok && v.sig_ok == Some(true)),
            None => (None, false),
        };
        if hash != Some(good) {
            moved.push(off);
        }
        if !verified {
            rejected.push(off);
        }
    }

    // (1) one contiguous run, starting immediately after the 64-byte signature.
    let (lo, hi) = (moved[0], *moved.last().unwrap());
    assert_eq!(lo, 64, "block data must start right after the signature");
    assert_eq!(moved.len(), hi - lo + 1, "the data region must be contiguous");

    // (2) every byte of block content is signed.
    for off in &moved {
        assert!(
            rejected.contains(off),
            "byte {off} changes block data yet the shred still verifies — a substitution \
             there would be invisible"
        );
    }

    // (3) some auth bytes break the signature while leaving block data intact.
    let auth: Vec<usize> = rejected
        .iter()
        .copied()
        .filter(|o| *o >= 64 && !moved.contains(o))
        .collect();
    assert!(
        !auth.is_empty(),
        "no byte fails verification while leaving the block data provably intact — then \
         invalid_sig could never occur and the whole split would be vacuous"
    );

    // (4) the tail outside the data region is exactly what the variant declares.
    let variant = payload[64];
    let proof_size = (variant & 0x0f) as usize;
    let (chained, resigned) = match variant & 0xf0 {
        0x80 => (false, false),
        0x90 => (true, false),
        0xb0 => (true, true),
        v => panic!("unexpected data-shred variant {v:#x}"),
    };
    let expected_tail =
        32 * usize::from(chained) + 20 * proof_size + 64 * usize::from(resigned);
    assert_eq!(
        payload.len() - (hi + 1),
        expected_tail,
        "the bytes past the data region must be exactly the chained root + merkle proof \
         + retransmitter signature the variant byte declares"
    );
}

#[test]
fn ping_with_a_bad_signature_is_not_excused_as_a_ping() {
    let sender = Keypair::new();
    let mut ping = make_ping(&sender, 0x80);
    ping[70] ^= 0x01; // corrupt the signature

    let sched = LeaderSchedule::for_test(1000, vec![Some(Keypair::new().pubkey())]);
    let mut stats = VerifyStats::default();
    verify_chunk(&packets(&[ping]), &sched, None, &mut stats);

    assert_eq!(stats.non_shred_ping, 0, "only a verified ping may be called a ping");
    assert_eq!(stats.malformed, 1, "anything else stays malformed");
}

#[test]
fn real_leader_signed_shreds_verify_ok() {
    let leader = Keypair::new();
    let slot = 1000;
    let shreds = make_real_shreds(slot, &leader);
    assert!(shreds.len() >= 2, "shredder should emit data+coding shreds");

    let sched = LeaderSchedule::for_test(slot, vec![Some(leader.pubkey())]);
    let mut stats = VerifyStats::default();
    let out = verify_chunk(&packets(&shreds), &sched, Some(42), &mut stats);

    assert_eq!(out.len(), shreds.len(), "all shreds should parse");
    assert!(out.iter().all(|s| s.merkle_ok), "every shred's merkle root should reconstruct");
    assert!(
        out.iter().all(|s| s.sig_ok == Some(true)),
        "every shred should verify against the real leader"
    );
    assert_eq!(stats.sig_bad, 0);
    assert!(stats.ed25519_verifies >= 1, "at least one real ed25519 verify should run");
    assert!(
        stats.ed25519_verifies <= out.len() as u64,
        "verify count should not exceed shred count"
    );
}

#[test]
fn flipped_signature_is_rejected() {
    let leader = Keypair::new();
    let slot = 1000;
    let mut shreds = make_real_shreds(slot, &leader);
    for s in &mut shreds {
        s[10] ^= 0xFF;
    }
    let sched = LeaderSchedule::for_test(slot, vec![Some(leader.pubkey())]);
    let mut stats = VerifyStats::default();
    let out = verify_chunk(&packets(&shreds), &sched, Some(42), &mut stats);

    assert!(
        out.iter().all(|s| s.sig_ok == Some(false)),
        "a flipped signature byte must make every shred fail verification"
    );
    assert!(stats.sig_bad >= 1);
}

#[test]
fn wrong_leader_is_rejected() {
    let leader = Keypair::new();
    let impostor = Keypair::new();
    let slot = 1000;
    let shreds = make_real_shreds(slot, &leader);

    let sched = LeaderSchedule::for_test(slot, vec![Some(impostor.pubkey())]);
    let mut stats = VerifyStats::default();
    let out = verify_chunk(&packets(&shreds), &sched, Some(42), &mut stats);

    assert!(
        out.iter().all(|s| s.sig_ok == Some(false)),
        "shreds signed by the real leader must fail against a different pubkey"
    );
}

#[test]
fn unknown_leader_is_unverifiable_not_bad() {
    let leader = Keypair::new();
    let slot = 1000;
    let shreds = make_real_shreds(slot, &leader);

    let sched = LeaderSchedule::for_test(slot + 10_000, vec![Some(leader.pubkey())]);
    let mut stats = VerifyStats::default();
    let out = verify_chunk(&packets(&shreds), &sched, Some(42), &mut stats);

    assert!(out.iter().all(|s| s.sig_ok.is_none()), "no leader -> no verdict");
    assert_eq!(stats.sig_bad, 0, "unknown leader must never be counted as a bad signature");
    assert!(stats.no_leader >= 1);
}

#[test]
fn an_unparseable_variant_is_counted_not_condemned() {
    let leader = Keypair::new();
    let slot = 1000;
    let sched = LeaderSchedule::for_test(slot, vec![Some(leader.pubkey())]);
    let real = make_real_shreds(slot, &leader);

    // 0xa5 = legacy data variant; 0xc3 = nonexistent variant.
    for variant in [0xa5u8, 0xc3u8] {
        let mut s = real[0].clone();
        s[64] = variant;

        let mut stats = VerifyStats::default();
        let out = verify_chunk(&packets(&[s]), &sched, None, &mut stats);

        assert!(out.is_empty(), "variant {variant:#04x} must not reach the aggregator");
        assert_eq!(stats.unsupported_variant, 1, "variant {variant:#04x} must be counted");
        assert_eq!(
            stats.sig_bad, 0,
            "variant {variant:#04x} must never be reported as a bad signature"
        );
        assert_eq!(stats.no_merkle_root, 0, "we never even attempted the merkle check");
        assert_eq!(stats.parsed, 0);
    }
}
