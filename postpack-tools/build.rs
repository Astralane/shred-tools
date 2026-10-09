use std::path::PathBuf;

pub fn main() {
    // Jito and Gravity reuse package names (auth, packet, bundle, shared) with different
    // messages, so each set is generated into its own directory.
    let out_dir = PathBuf::from(std::env::var("OUT_DIR").unwrap());

    let jito = out_dir.join("jito");
    std::fs::create_dir_all(&jito).unwrap();
    tonic_prost_build::configure()
        .out_dir(&jito)
        .bytes(".packet.Packet.data")
        .bytes(".bam_preconf.PreconfUpdate.transaction")
        .bytes(".bam_preconf.PreconfSubscribeRequest.signature")
        .compile_protos(
            &[
                "protos/jito/astralane_relayer.proto",
                "protos/jito/auth.proto",
                "protos/jito/bam_preconf.proto",
                "protos/jito/block_engine.proto",
                "protos/jito/bundle.proto",
                "protos/jito/packet.proto",
                "protos/jito/shared.proto",
            ],
            &["protos/jito"],
        )
        .unwrap();

    let gravity = out_dir.join("gravity");
    std::fs::create_dir_all(&gravity).unwrap();
    tonic_prost_build::configure()
        .out_dir(&gravity)
        .bytes(".packet.Packet.data")
        .bytes(".bundle.JitoBundle.bundle_id")
        .bytes(".bundle.JitoBundle.identity_address")
        .bytes(".searcher.SendBackrunBundleRequest.target_bundle_id")
        .bytes(".searcher.PendingTxNotification.identity_address")
        .compile_protos(
            &[
                "protos/gravity/auth.proto",
                "protos/gravity/bundle.proto",
                "protos/gravity/packet.proto",
                "protos/gravity/searcher.proto",
                "protos/gravity/shared.proto",
            ],
            &["protos/gravity"],
        )
        .unwrap();
}
