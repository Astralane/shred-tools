use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use bytes::Bytes;
use crossbeam_channel::Receiver;
use serde::Serialize;

use crate::sigreg::{is_simple_vote, SigRegistry, TxVersion, TxnMeta};

pub struct ShredInput {
    pub rx_unix_ns: i64,
    pub provider: u16,
    pub data: Bytes,
}

/// Per-provider outcome of the real reconstruction. Every field is cumulative.
#[derive(Default, Clone, Copy, Serialize)]
pub struct ProviderDeshredStats {
    /// Authenticated shreds that reached this provider's deshredder.
    pub deshred_fed: u64,
    /// Authenticated shreds lost because the deshred queue was full: our loss, not the provider's.
    pub deshred_dropped: u64,
    /// Shreds that passed verification but that agave would not parse.
    pub deshred_parse_errors: u64,
    /// Reed-Solomon attempts that failed or rebuilt an unparseable shred.
    pub deshred_recovery_errors: u64,
    /// Complete data sets that would not deshred or deserialize into entries.
    pub deshred_decode_errors: u64,
    /// Complete data sets decoded into entries.
    pub deshred_data_sets: u64,
}

impl ProviderDeshredStats {
    pub fn errors(&self) -> u64 {
        self.deshred_parse_errors + self.deshred_recovery_errors + self.deshred_decode_errors
    }

    /// Per-field `self - prev`; a counter never runs backwards, so this is an interval delta.
    pub fn since(&self, prev: &Self) -> Self {
        Self {
            deshred_fed: self.deshred_fed.saturating_sub(prev.deshred_fed),
            deshred_dropped: self.deshred_dropped.saturating_sub(prev.deshred_dropped),
            deshred_parse_errors: self.deshred_parse_errors.saturating_sub(prev.deshred_parse_errors),
            deshred_recovery_errors: self
                .deshred_recovery_errors
                .saturating_sub(prev.deshred_recovery_errors),
            deshred_decode_errors: self.deshred_decode_errors.saturating_sub(prev.deshred_decode_errors),
            deshred_data_sets: self.deshred_data_sets.saturating_sub(prev.deshred_data_sets),
        }
    }
}

#[derive(Default)]
struct ProviderCounters {
    fed: AtomicU64,
    dropped: AtomicU64,
    parse_errors: AtomicU64,
    recovery_errors: AtomicU64,
    decode_errors: AtomicU64,
    data_sets: AtomicU64,
}

/// Written by the feeding thread (`dropped`) and the deshred thread (everything else).
pub struct DeshredMetrics {
    providers: Box<[ProviderCounters]>,
}

impl DeshredMetrics {
    pub fn new(providers: usize) -> Self {
        Self {
            providers: (0..providers).map(|_| ProviderCounters::default()).collect(),
        }
    }

    pub fn record_dropped(&self, provider: u16) {
        if let Some(p) = self.providers.get(provider as usize) {
            p.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn snapshot(&self) -> Vec<ProviderDeshredStats> {
        self.providers
            .iter()
            .map(|p| ProviderDeshredStats {
                deshred_fed: p.fed.load(Ordering::Relaxed),
                deshred_dropped: p.dropped.load(Ordering::Relaxed),
                deshred_parse_errors: p.parse_errors.load(Ordering::Relaxed),
                deshred_recovery_errors: p.recovery_errors.load(Ordering::Relaxed),
                deshred_decode_errors: p.decode_errors.load(Ordering::Relaxed),
                deshred_data_sets: p.data_sets.load(Ordering::Relaxed),
            })
            .collect()
    }
}

pub struct Deshredder {
    reg: Arc<Mutex<SigRegistry>>,
    metrics: Arc<DeshredMetrics>,
    streams: HashMap<u16, ::deshred::Deshredder>,
}

impl Deshredder {
    pub fn new(reg: Arc<Mutex<SigRegistry>>, metrics: Arc<DeshredMetrics>) -> Self {
        Self {
            reg,
            metrics,
            streams: HashMap::new(),
        }
    }

    pub fn run(mut self, rx: Receiver<ShredInput>) {
        for input in rx {
            self.ingest(input);
        }
    }

    fn ingest(&mut self, input: ShredInput) {
        let Some(counters) = self.metrics.providers.get(input.provider as usize) else {
            return;
        };
        counters.fed.fetch_add(1, Ordering::Relaxed);
        let stream = self.streams.entry(input.provider).or_default();
        let completed = match stream.insert_bytes(input.data) {
            Ok(completed) => completed,
            Err(_) => {
                counters.parse_errors.fetch_add(1, Ordering::Relaxed);
                return;
            }
        };
        // The stream's own counters are cumulative and this thread is their only writer.
        let stats = stream.stats();
        counters.recovery_errors.store(stats.recovery_errors, Ordering::Relaxed);
        counters.decode_errors.store(stats.decode_errors, Ordering::Relaxed);
        if completed.is_empty() {
            return;
        }
        counters.data_sets.fetch_add(completed.len() as u64, Ordering::Relaxed);
        let mut reg = self.reg.lock().unwrap();
        for set in &completed {
            for tx in set.transactions() {
                let Some(sig) = ::deshred::first_signature(tx) else {
                    continue;
                };
                let is_vote = is_vote(tx);
                let meta = TxnMeta {
                    is_vote: Some(is_vote),
                    version: Some(version(tx, is_vote)),
                    ..TxnMeta::default()
                };
                reg.record_first(input.provider as usize, sig, input.rx_unix_ns, set.slot, meta);
            }
        }
    }
}

fn version(tx: &::deshred::VersionedTransaction, is_vote: bool) -> TxVersion {
    match &tx.message {
        ::deshred::VersionedMessage::Legacy(_) if is_vote => TxVersion::Vote,
        ::deshred::VersionedMessage::Legacy(_) => TxVersion::Legacy,
        ::deshred::VersionedMessage::V0(_) => TxVersion::V0,
        ::deshred::VersionedMessage::V1(_) => TxVersion::V1,
    }
}

fn is_vote(tx: &::deshred::VersionedTransaction) -> bool {
    let ix = tx.message.instructions();
    let program_id: Option<&[u8]> = ix
        .first()
        .and_then(|i| tx.message.static_account_keys().get(i.program_id_index as usize))
        .map(|k| k.as_ref());
    let legacy = tx.message.address_table_lookups().is_none();
    is_simple_vote(tx.signatures.len(), legacy, ix.len(), program_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sigreg::SourceKind;

    fn deshredder(providers: usize) -> (Deshredder, Arc<DeshredMetrics>) {
        let names = (0..providers).map(|i| format!("p{i}")).collect();
        let reg = SigRegistry::new(names, vec![SourceKind::Shred; providers]);
        let metrics = Arc::new(DeshredMetrics::new(providers));
        (Deshredder::new(Arc::new(Mutex::new(reg)), metrics.clone()), metrics)
    }

    #[test]
    fn an_unparseable_shred_is_a_per_provider_parse_error() {
        let (mut d, metrics) = deshredder(2);
        d.ingest(ShredInput {
            rx_unix_ns: 1,
            provider: 1,
            data: Bytes::from_static(&[0u8; 100]),
        });

        let snap = metrics.snapshot();
        assert_eq!((snap[1].deshred_fed, snap[1].deshred_parse_errors), (1, 1));
        assert_eq!(snap[1].errors(), 1);
        assert_eq!(snap[0].deshred_fed, 0, "only the sending provider is charged");
    }

    #[test]
    fn drops_are_counted_apart_from_errors() {
        let metrics = DeshredMetrics::new(1);
        metrics.record_dropped(0);
        metrics.record_dropped(7); // unknown provider: ignored, never panics

        let snap = metrics.snapshot();
        assert_eq!(snap[0].deshred_dropped, 1);
        assert_eq!(snap[0].errors(), 0);
    }

    #[test]
    fn since_is_an_interval_delta() {
        let prev = ProviderDeshredStats { deshred_fed: 10, deshred_decode_errors: 1, ..Default::default() };
        let now = ProviderDeshredStats { deshred_fed: 25, deshred_decode_errors: 3, ..Default::default() };
        let d = now.since(&prev);
        assert_eq!((d.deshred_fed, d.deshred_decode_errors), (15, 2));
    }
}
