pub fn main() {
    tonic_prost_build::configure()
        .compile_protos(&["geyser.proto", "solana-storage.proto"], &["."])
        .unwrap();
}
