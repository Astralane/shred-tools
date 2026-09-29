use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use crossbeam_channel::Receiver;

use crate::sigreg::{is_simple_vote, SigRegistry, TxnMeta};

pub struct ShredInput {
    pub rx_unix_ns: i64,
    pub provider: u16,
    pub data: Bytes,
}

pub struct Deshredder {
    reg: Arc<Mutex<SigRegistry>>,
    max_wait_slots: u64,
    streams: HashMap<u16, ::deshred::Deshredder>,
}

impl Deshredder {
    pub fn new(reg: Arc<Mutex<SigRegistry>>, max_wait_slots: u64) -> Self {
        Self {
            reg,
            max_wait_slots,
            streams: HashMap::new(),
        }
    }

    pub fn run(mut self, rx: Receiver<ShredInput>) {
        for input in rx {
            self.ingest(input);
        }
    }

    fn ingest(&mut self, input: ShredInput) {
        let max_wait_slots = self.max_wait_slots;
        let completed = self
            .streams
            .entry(input.provider)
            .or_insert_with(|| ::deshred::Deshredder::new(max_wait_slots))
            .insert(input.data)
            .data_sets;
        if completed.is_empty() {
            return;
        }
        let mut reg = self.reg.lock().unwrap();
        for set in &completed {
            for tx in set.transactions() {
                let Some(sig) = ::deshred::first_signature(tx) else {
                    continue;
                };
                let meta = TxnMeta {
                    is_vote: Some(is_vote(tx)),
                    ..TxnMeta::default()
                };
                reg.record_first(input.provider as usize, sig, input.rx_unix_ns, set.slot, meta);
            }
        }
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
