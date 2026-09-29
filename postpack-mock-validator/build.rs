pub fn main() {
    tonic_prost_build::configure()
        .build_server(false)
        .bytes(".packet.Packet.data")
        .compile_protos(
            &[
                "protos/astralane_relayer.proto",
                "protos/auth.proto",
                "protos/block_engine.proto",
                "protos/bundle.proto",
                "protos/packet.proto",
                "protos/shared.proto",
            ],
            &["protos"],
        )
        .unwrap();
}
