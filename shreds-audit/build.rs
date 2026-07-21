//! Compile the Yellowstone/Geyser gRPC protobufs into Rust at build time.
//!
//! Only needed for the optional shred-vs-gRPC transaction-timing comparison. The
//! protoc binary is vendored so the build does not depend on a system install.

use std::{env, path::PathBuf};

const PROTO_ROOT: &str = "proto";
const PROTO_FILES: &[&str] = &["proto/geyser.proto", "proto/solana-storage.proto"];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto");

    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    let include_path = protoc_bin_vendored::include_path()?;
    // Some prost/tonic build paths still read PROTOC from the environment.
    unsafe {
        env::set_var("PROTOC", &protoc);
        env::set_var("PROTOC_INCLUDE", &include_path);
    }

    let out_dir = PathBuf::from(env::var("OUT_DIR")?);
    let include_dir = include_path.to_string_lossy().into_owned();
    let includes = [PROTO_ROOT, include_dir.as_str()];

    tonic_prost_build::configure()
        .build_server(false)
        .file_descriptor_set_path(out_dir.join("proto_descriptors.bin"))
        .compile_protos(PROTO_FILES, &includes)?;
    Ok(())
}
