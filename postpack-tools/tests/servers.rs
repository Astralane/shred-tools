//! Starts the server mocks and subscribes the way the relay does
//! (`mev-relay/src/postpack/bam_ingress.rs`, `mev-relay/src/gravity_relay_client.rs`).

use std::net::TcpListener;
use std::process::{Child, Command};
use std::time::Duration;

use solana_keypair::Keypair;
use solana_signer::Signer;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::{Code, Request};

use postpack_tools::proto::gravity::auth::auth_service_client::AuthServiceClient;
use postpack_tools::proto::gravity::auth::{
    GenerateAuthChallengeRequest, GenerateAuthTokensRequest, Role,
};
use postpack_tools::proto::gravity::searcher::searcher_service_client::SearcherServiceClient;
use postpack_tools::proto::gravity::searcher::{BundleSubscription, MempoolSubscription};
use postpack_tools::proto::jito::bam_preconf::PreconfSubscribeRequest;
use postpack_tools::proto::jito::bam_preconf::preconf_stream_client::PreconfStreamClient;

struct Mock(Child);

impl Drop for Mock {
    fn drop(&mut self) {
        let _ = self.0.kill();
    }
}

async fn start(binary: &str, extra: &[&str]) -> (Mock, Channel) {
    let port = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let listen = format!("127.0.0.1:{port}");
    let mock = Mock(
        Command::new(binary)
            .args(["--listen", &listen, "--rate", "50", "--batch-size", "3"])
            .args(extra)
            .spawn()
            .unwrap(),
    );
    for _ in 0..100 {
        if let Ok(channel) = Channel::from_shared(format!("http://{listen}"))
            .unwrap()
            .connect()
            .await
        {
            return (mock, channel);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("{binary} did not start");
}

fn bam_request(keypair: &Keypair, signer: &Keypair) -> Request<PreconfSubscribeRequest> {
    let signed_slot = 42u64;
    let mut request = Request::new(PreconfSubscribeRequest {
        signed_slot,
        signature: bytes::Bytes::copy_from_slice(
            signer.sign_message(&signed_slot.to_le_bytes()).as_ref(),
        ),
    });
    request.metadata_mut().insert(
        "x-bam-pubkey",
        MetadataValue::try_from(keypair.pubkey().to_string()).unwrap(),
    );
    request
}

#[tokio::test]
async fn bam_streams_bundles_of_preconfirmed_transactions() {
    let (_mock, channel) = start(env!("CARGO_BIN_EXE_mock-bam"), &["--slot", "7"]).await;
    let keypair = Keypair::new();
    let mut stream = PreconfStreamClient::new(channel)
        .subscribe(bam_request(&keypair, &keypair))
        .await
        .unwrap()
        .into_inner();

    let first = stream.message().await.unwrap().unwrap();
    let mut positions = vec![first.bundle_position];
    while positions.len() < 3 {
        let update = stream.message().await.unwrap().unwrap();
        if update.sequence_id == first.sequence_id {
            positions.push(update.bundle_position);
        } else {
            positions = vec![update.bundle_position];
        }
    }
    assert_eq!(first.slot, 7);
    assert!(!first.transaction.is_empty());
    assert!(positions.contains(&0) && positions.contains(&2));
}

#[tokio::test]
async fn bam_rejects_a_signature_from_another_key() {
    let (_mock, channel) = start(env!("CARGO_BIN_EXE_mock-bam"), &[]).await;
    let status = PreconfStreamClient::new(channel)
        .subscribe(bam_request(&Keypair::new(), &Keypair::new()))
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::Unauthenticated);
}

#[tokio::test]
async fn gravity_authenticates_a_searcher_and_streams_both_feeds() {
    let identity = Keypair::new().pubkey();
    let (_mock, channel) = start(
        env!("CARGO_BIN_EXE_mock-gravity"),
        &["--identity", &identity.to_string()],
    )
    .await;

    let mut searcher = SearcherServiceClient::new(channel.clone());
    let unauthorized = searcher
        .subscribe_mempool(MempoolSubscription {})
        .await
        .unwrap_err();
    assert_eq!(unauthorized.code(), Code::Unauthenticated);

    let keypair = Keypair::new();
    let mut auth = AuthServiceClient::new(channel);
    let challenge = auth
        .generate_auth_challenge(GenerateAuthChallengeRequest {
            role: Role::Searcher as i32,
            pubkey: keypair.pubkey().to_bytes().to_vec(),
        })
        .await
        .unwrap()
        .into_inner()
        .challenge;
    let challenge = format!("{}-{challenge}", keypair.pubkey());
    let token = auth
        .generate_auth_tokens(GenerateAuthTokensRequest {
            signed_challenge: keypair.sign_message(challenge.as_bytes()).as_ref().to_vec(),
            challenge,
            client_pubkey: keypair.pubkey().to_bytes().to_vec(),
        })
        .await
        .unwrap()
        .into_inner()
        .access_token
        .unwrap()
        .value;

    let mut mempool = searcher
        .subscribe_mempool(authed(MempoolSubscription {}, &token))
        .await
        .unwrap()
        .into_inner();
    let notification = loop {
        let message = mempool.message().await.unwrap().unwrap();
        if !message.transactions.is_empty() {
            break message;
        }
    };
    assert_eq!(notification.identity_address.as_ref(), identity.as_ref());
    assert_eq!(notification.transactions.len(), 3);
    assert!(
        notification.transactions[0].data[1..65]
            .iter()
            .any(|b| *b != 0)
    );

    let mut bundles = searcher
        .subscribe_bundles(authed(BundleSubscription {}, &token))
        .await
        .unwrap()
        .into_inner();
    let bundle = loop {
        let message = bundles.message().await.unwrap().unwrap();
        if !message.packets.is_empty() {
            break message;
        }
    };
    assert_eq!(bundle.bundle_id.len(), 32);
    assert_eq!(bundle.identity_address.as_ref(), identity.as_ref());
    assert!(bundle.packets[0].data[1..65].iter().all(|b| *b == 0));
}

fn authed<T>(message: T, token: &str) -> Request<T> {
    let mut request = Request::new(message);
    request.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from(format!("Bearer {token}")).unwrap(),
    );
    request
}
