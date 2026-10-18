use meta::{
    factory::Factory,
    layers::{
        overlay::{
            context::OverlayContext, factory::OverlayFactory, gossip::node::process_inbound_stream,
        },
        tcp::factory::TcpFactory,
    },
};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::time::sleep;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("============================================================");
    println!("  P2Pマルチネットワーク（VNet）＆ ストリーム通信デモ");
    println!("============================================================\n");

    let bob_tcp: SocketAddr = "127.0.0.1:19001".parse()?;
    let alice_tcp: SocketAddr = "127.0.0.1:19002".parse()?;
    let eve_tcp: SocketAddr = "127.0.0.1:19003".parse()?;

    // ------------------------------------------------------------
    // 2つの異なる仮想ネットワークID (Uuid) を定義
    // ------------------------------------------------------------
    let secure_vnet_id = Uuid::new_v4();
    let guest_vnet_id = Uuid::new_v4();

    println!("仮想ネットワーク [Secure VNet] ID: {}", secure_vnet_id);
    println!("仮想ネットワーク [Guest  VNet] ID: {}\n", guest_vnet_id);

    // 1. Bob: Secure VNet に参加 (サーバー役)
    let bob_ctx =
        OverlayContext::with_network(secure_vnet_id, TcpFactory, vec![alice_tcp, eve_tcp]);
    start_tcp_listener(bob_tcp, bob_ctx.clone()).await;
    let bob_factory = OverlayFactory::new(bob_ctx.clone());

    println!("Bob   [Secure VNet] 起動: ID={}", bob_ctx.address());

    // 2. Alice: Secure VNet に参加 (クライアント役)
    let alice_ctx = OverlayContext::with_network(secure_vnet_id, TcpFactory, vec![bob_tcp]);
    start_tcp_listener(alice_tcp, alice_ctx.clone()).await;
    let alice_factory = OverlayFactory::new(alice_ctx.clone());

    println!("Alice [Secure VNet] 起動: ID={}", alice_ctx.address());

    // 3. Eve: Guest VNet に参加 (別ネットワーク所属の第三者)
    let eve_ctx = OverlayContext::with_network(guest_vnet_id, TcpFactory, vec![bob_tcp]);
    start_tcp_listener(eve_tcp, eve_ctx.clone()).await;

    println!("Eve   [Guest  VNet] 起動: ID={}\n", eve_ctx.address());

    // ------------------------------------------------------------
    // Bob (サーバー) が Secure VNet 上でストリームをリッスン
    // ------------------------------------------------------------
    let mut bob_listener = bob_factory.listen();
    let bob_handle = tokio::spawn(async move {
        println!("[Bob] Secure VNet 上で接続待機中 (listener.accept().await)...");
        let (mut stream, peer) = bob_listener.accept().await.unwrap();
        println!("[Bob] 接続を受け付けました！ (Peer: {})", peer);

        let mut buf = [0u8; 1024];
        let n = stream.read(&mut buf).await.unwrap();
        println!(
            "[Bob] ストリームから受信: \"{}\"",
            String::from_utf8_lossy(&buf[..n])
        );

        stream
            .write_all(b"Hello Alice! Secret data confirmed.")
            .await
            .unwrap();
    });

    // Eve (Guest VNet) もリスナーを起動して盗聴を試みる
    let mut eve_listener = eve_ctx.listen();
    let eve_handle = tokio::spawn(async move {
        // Secure VNet のメッセージは Eve の上位リスナーには届かない（タイムアウトする）
        let result = tokio::time::timeout(Duration::from_millis(500), eve_listener.accept()).await;
        match result {
            Err(_) => println!("🛡️ [Eve] 隔離成功: Secure VNet の通信は一切受信できませんでした"),
            Ok(_) => println!("⚠️ [Eve] 警告: 誤って受信してしまいました"),
        }
    });

    sleep(Duration::from_millis(150)).await;

    // ------------------------------------------------------------
    // Alice が Bob (Secure VNet) へ仮想ストリーム接続
    // ------------------------------------------------------------
    println!("[Alice] Bobの公開鍵へ仮想ストリーム接続中 (factory.connect)...");
    let mut alice_stream = alice_factory.connect(bob_ctx.address()).await?;
    println!("[Alice] ストリーム確立完了！\n");

    let message = b"Top secret payload for Secure VNet only!";
    println!(
        "[Alice] ストリームへ送信: \"{}\"",
        String::from_utf8_lossy(message)
    );
    alice_stream.write_all(message).await?;

    let mut reply = [0u8; 1024];
    let n = alice_stream.read(&mut reply).await?;
    println!(
        "[Alice] Bobからの返信を受信: \"{}\"\n",
        String::from_utf8_lossy(&reply[..n])
    );

    bob_handle.await?;
    eve_handle.await?;

    println!("\n============================================================");
    println!("  マルチネットワーク分離＆ストリーム通信に成功しました！");
    println!("============================================================");

    Ok(())
}
