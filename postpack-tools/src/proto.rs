//! Wire types of every provider the relay ingests from.

/// Jito-derived protos: the Astralane relayer patch, the block-engine relayer and BAM.
#[allow(dead_code, clippy::all)]
pub mod jito {
    pub mod astralane_relayer {
        include!(concat!(env!("OUT_DIR"), "/jito/astralane_relayer.rs"));
    }
    pub mod auth {
        include!(concat!(env!("OUT_DIR"), "/jito/auth.rs"));
    }
    pub mod bam_preconf {
        include!(concat!(env!("OUT_DIR"), "/jito/bam_preconf.rs"));
    }
    pub mod block_engine {
        include!(concat!(env!("OUT_DIR"), "/jito/block_engine.rs"));
    }
    pub mod bundle {
        include!(concat!(env!("OUT_DIR"), "/jito/bundle.rs"));
    }
    pub mod packet {
        include!(concat!(env!("OUT_DIR"), "/jito/packet.rs"));
    }
    pub mod shared {
        include!(concat!(env!("OUT_DIR"), "/jito/shared.rs"));
    }
}

/// Gravity relay protos, vendored in txn-router from gattaca-external/solana-relay.
#[allow(dead_code, clippy::all)]
pub mod gravity {
    pub mod auth {
        include!(concat!(env!("OUT_DIR"), "/gravity/auth.rs"));
    }
    pub mod bundle {
        include!(concat!(env!("OUT_DIR"), "/gravity/bundle.rs"));
    }
    pub mod packet {
        include!(concat!(env!("OUT_DIR"), "/gravity/packet.rs"));
    }
    pub mod searcher {
        include!(concat!(env!("OUT_DIR"), "/gravity/searcher.rs"));
    }
    pub mod shared {
        include!(concat!(env!("OUT_DIR"), "/gravity/shared.rs"));
    }
}
