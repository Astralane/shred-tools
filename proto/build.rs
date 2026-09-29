use std::{env, path::PathBuf};

pub fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=geyser.proto");
    println!("cargo:rerun-if-changed=solana-storage.proto");
    println!("cargo:rerun-if-changed=yellowstone");

    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    let include_path = protoc_bin_vendored::include_path()?;
    unsafe {
        env::set_var("PROTOC", &protoc);
        env::set_var("PROTOC_INCLUDE", &include_path);
    }

    let out_dir = PathBuf::from(env::var("OUT_DIR")?);
    let include_dir = include_path.to_string_lossy().into_owned();

    tonic_prost_build::configure()
        .compile_protos(&["geyser.proto", "solana-storage.proto"], &[".", &include_dir])?;

    let yellowstone_out = out_dir.join("yellowstone");
    std::fs::create_dir_all(&yellowstone_out)?;
    tonic_prost_build::configure()
        .build_server(false)
        .out_dir(&yellowstone_out)
        .compile_protos(
            &["yellowstone/geyser.proto", "yellowstone/solana-storage.proto"],
            &["yellowstone", &include_dir],
        )?;

    Ok(())
}
