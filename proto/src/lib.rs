pub mod solana {
    pub mod storage {
        pub mod confirmed_block {
            tonic::include_proto!("solana.storage.confirmed_block");
        }
    }
}

pub mod geyser {
    tonic::include_proto!("geyser");
}
