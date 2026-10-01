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

#[allow(clippy::all, non_camel_case_types, non_snake_case, dead_code)]
pub mod yellowstone {
    pub mod solana {
        pub mod storage {
            pub mod confirmed_block {
                include!(concat!(
                    env!("OUT_DIR"),
                    "/yellowstone/solana.storage.confirmed_block.rs"
                ));
            }
        }
    }

    pub mod geyser {
        include!(concat!(env!("OUT_DIR"), "/yellowstone/geyser.rs"));
    }
}
