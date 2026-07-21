//! Generated protobuf bindings for the Geyser gRPC subscription. See `build.rs`.
#![allow(clippy::all, dead_code)]

#[allow(clippy::all, non_camel_case_types, non_snake_case)]
pub mod geyser {
    include!(concat!(env!("OUT_DIR"), "/geyser.rs"));
}

#[allow(clippy::all, non_camel_case_types, non_snake_case)]
pub mod solana {
    pub mod storage {
        pub mod confirmed_block {
            include!(concat!(env!("OUT_DIR"), "/solana.storage.confirmed_block.rs"));
        }
    }
}
