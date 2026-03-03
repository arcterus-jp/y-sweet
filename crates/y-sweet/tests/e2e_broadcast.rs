//! E2E tests for WebSocket broadcast fan-out, CRDT sync, and Awareness sync.
//!
//! Each test spins up a fully in-memory y-sweet server on a random port,
//! connects real WebSocket clients via tokio-tungstenite, and exercises the
//! y-sync handshake + broadcast pipeline end-to-end.

use futures::stream::{SplitSink, SplitStream};
use futures::{SinkExt, StreamExt};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_tungstenite::{
    connect_async, tungstenite::Message as WsMessage, MaybeTlsStream, WebSocketStream,
};
use tokio_util::sync::CancellationToken;
use y_sweet::server::Server;
use y_sweet_core::sync::awareness::Awareness;
use y_sweet_core::sync::{Message as YMessage, SyncMessage as YSyncMessage};
use yrs::{
    updates::{decoder::Decode, encoder::Encode},
    Doc, GetString, ReadTxn, Text, Transact, Update,
};

// ── Type alias ────────────────────────────────────────────────────────────────

type WsStream = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

// ── YClient ───────────────────────────────────────────────────────────────────

/// WebSocket client that wraps the y-sync protocol handshake.
struct YClient {
    sink: SplitSink<WsStream, WsMessage>,
    stream: SplitStream<WsStream>,
    pub doc: Doc,
}

impl YClient {
    /// Connect to `/d/{doc_id}/ws/{doc_id}` and complete the y-sync handshake.
    ///
    /// Handshake sequence (client-server model):
    /// 1. Server → `SyncStep1(server_sv)` (WS msg)
    /// 2. Server → `Awareness(update)` (WS msg)
    /// 3. Client → `SyncStep2(our_update_for_server)` + `SyncStep1(our_sv)`
    /// 4. Server → `SyncStep2(missing_state_for_client)` — apply to local doc
    ///
    /// Any interleaved `Update` messages arriving before step 4 completes are
    /// applied to the local doc immediately.
    async fn connect(addr: SocketAddr, doc_id: &str, client_id: u64) -> Self {
        let url = format!("ws://127.0.0.1:{}/d/{}/ws/{}", addr.port(), doc_id, doc_id);
        let (ws, _) = connect_async(&url).await.expect("WebSocket connect failed");

        let doc = Doc::with_client_id(client_id);
        let (sink, stream) = ws.split();
        let mut client = Self { sink, stream, doc };

        // Step 1: receive SyncStep1 from server
        let raw = client
            .recv_raw()
            .await
            .expect("Expected SyncStep1 from server");
        let server_sv = match YMessage::decode_v1(&raw).expect("decode SyncStep1") {
            YMessage::Sync(YSyncMessage::SyncStep1(sv)) => sv,
            other => panic!("Expected SyncStep1 from server, got {:?}", other),
        };

        // Step 2: receive Awareness from server (just consume it)
        let _ = client
            .recv_raw()
            .await
            .expect("Expected Awareness from server");

        // Step 3a: send SyncStep2 — our full state so the server can apply it
        let our_update = client.doc.transact().encode_state_as_update_v1(&server_sv);
        let step2_msg = YMessage::Sync(YSyncMessage::SyncStep2(our_update)).encode_v1();
        client
            .sink
            .send(WsMessage::Binary(step2_msg))
            .await
            .expect("send SyncStep2");

        // Step 3b: send SyncStep1 — ask server for anything we are missing
        let our_sv = client.doc.transact().state_vector();
        let step1_msg = YMessage::Sync(YSyncMessage::SyncStep1(our_sv)).encode_v1();
        client
            .sink
            .send(WsMessage::Binary(step1_msg))
            .await
            .expect("send SyncStep1");

        // Step 4: wait for SyncStep2 from server (may be interleaved with Updates)
        loop {
            let raw = client
                .recv_raw()
                .await
                .expect("Expected SyncStep2/Update from server during handshake");
            match YMessage::decode_v1(&raw).expect("decode handshake msg") {
                YMessage::Sync(YSyncMessage::SyncStep2(bytes)) => {
                    let update = Update::decode_v1(&bytes).expect("decode SyncStep2 update");
                    client.doc.transact_mut().apply_update(update);
                    break;
                }
                YMessage::Sync(YSyncMessage::Update(bytes)) => {
                    // Interleaved broadcast update — apply it
                    let update = Update::decode_v1(&bytes).expect("decode interleaved update");
                    client.doc.transact_mut().apply_update(update);
                }
                _ => {} // Awareness or other — ignore during handshake
            }
        }

        client
    }

    /// Receive the next raw binary WebSocket frame, skipping control frames.
    /// Returns `None` if the connection closed.
    async fn recv_raw(&mut self) -> Option<Vec<u8>> {
        loop {
            match self.stream.next().await? {
                Ok(WsMessage::Binary(bytes)) => return Some(bytes),
                Ok(WsMessage::Close(_)) => return None,
                Err(_) => return None,
                Ok(_) => continue, // Ping, Pong, Text, Frame — skip
            }
        }
    }

    /// Receive the next y-sync message, with a timeout.
    /// Skips frames that fail to decode as y-sync (should not happen in practice).
    /// Returns `None` on timeout or connection close.
    async fn recv_message_timeout(&mut self, dur: Duration) -> Option<YMessage> {
        loop {
            let raw = match tokio::time::timeout(dur, self.recv_raw()).await {
                Ok(Some(r)) => r,
                _ => return None,
            };
            if let Ok(msg) = YMessage::decode_v1(&raw) {
                return Some(msg);
            }
        }
    }

    /// Encode and send a CRDT update delta as `Message::Sync(Update(...))`.
    async fn send_update(&mut self, bytes: Vec<u8>) {
        let msg = YMessage::Sync(YSyncMessage::Update(bytes)).encode_v1();
        self.sink
            .send(WsMessage::Binary(msg))
            .await
            .expect("send_update");
    }

    /// Build and send an Awareness update with the given JSON state.
    async fn send_awareness(&mut self, json_state: &str) {
        let client_id = self.doc.client_id();
        // Build a temporary Awareness to produce a properly encoded AwarenessUpdate.
        let doc = Doc::with_client_id(client_id);
        let mut awareness = Awareness::new(doc);
        awareness.set_local_state(json_state);
        let update = awareness.update().expect("awareness.update()");
        let msg = YMessage::Awareness(update).encode_v1();
        self.sink
            .send(WsMessage::Binary(msg))
            .await
            .expect("send_awareness");
    }

    /// Send a WebSocket Close frame and drop the connection.
    async fn close(mut self) {
        let _ = self.sink.close().await;
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Insert `content` at the end of the named text field in `doc`.
/// Returns the encoded delta update bytes to send to the server.
fn apply_text_insert(doc: &Doc, text_name: &str, content: &str) -> Vec<u8> {
    let text = doc.get_or_insert_text(text_name);
    let mut txn = doc.transact_mut();
    text.push(&mut txn, content);
    txn.encode_update_v1()
}

/// Read the current string value of the named text field in `doc`.
fn read_text(doc: &Doc, text_name: &str) -> String {
    let text = doc.get_or_insert_text(text_name);
    let txn = doc.transact();
    text.get_string(&txn)
}

/// Receive a `SyncMessage::Update` and apply it to the given doc.
/// Panics if no update arrives within the timeout.
async fn recv_and_apply_update(client: &mut YClient, timeout: Duration) {
    match client.recv_message_timeout(timeout).await {
        Some(YMessage::Sync(YSyncMessage::Update(bytes))) => {
            let update = Update::decode_v1(&bytes).expect("decode update");
            client.doc.transact_mut().apply_update(update);
        }
        Some(other) => panic!("Expected Update, got {:?}", other),
        None => panic!("Timeout waiting for Update"),
    }
}

/// Start a fully in-memory y-sweet server on an OS-assigned port.
/// Returns the bound address and a cancellation token to shut it down.
async fn start_server() -> (SocketAddr, CancellationToken) {
    let cancellation_token = CancellationToken::new();
    let server = Server::new(
        None,                   // store  — in-memory only
        Duration::from_secs(1), // checkpoint_freq
        None,                   // authenticator — no auth
        None,                   // url_prefix
        cancellation_token.clone(),
        true,  // doc_gc
        None,  // max_body_size
        false, // skip_gc
    )
    .await
    .expect("Server::new");

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind random port");
    let addr = listener.local_addr().expect("local_addr");

    tokio::spawn(async move {
        // ignore shutdown errors (CancellationToken triggers graceful shutdown)
        let _ = server.serve(listener, false).await;
    });

    (addr, cancellation_token)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// Test 1: Basic two-client CRDT sync.
///
/// Client A writes to a text field → Client B receives the broadcast update
/// and its local doc reflects the change.
#[tokio::test]
async fn test_basic_two_client_sync() {
    let (addr, _token) = start_server().await;

    let mut client_a = YClient::connect(addr, "doc1", 1).await;
    let mut client_b = YClient::connect(addr, "doc1", 2).await;

    // Client A inserts text and sends the delta to the server.
    let update = apply_text_insert(&client_a.doc, "content", "hello");
    client_a.send_update(update).await;

    // Client B receives the broadcast update and applies it.
    recv_and_apply_update(&mut client_b, Duration::from_secs(5)).await;

    assert_eq!(read_text(&client_b.doc, "content"), "hello");
}

/// Test 2: Broadcast fan-out to 8 clients (N=8 O(1) verification).
///
/// Client 0 writes → Clients 1–7 all receive the update exactly once.
#[tokio::test]
async fn test_multi_client_fan_out() {
    let (addr, _token) = start_server().await;

    let mut clients: Vec<YClient> =
        futures::future::join_all((0u64..8).map(|i| YClient::connect(addr, "doc2", 200 + i))).await;

    // Client 0 writes.
    let update = apply_text_insert(&clients[0].doc, "shared", "fan-out-test");
    clients[0].send_update(update).await;

    // Clients 1–7 each receive and apply the update.
    for i in 1..8 {
        recv_and_apply_update(&mut clients[i], Duration::from_secs(5)).await;
        assert_eq!(
            read_text(&clients[i].doc, "shared"),
            "fan-out-test",
            "client {} did not receive fan-out update",
            i
        );
    }
}

/// Test 3: Multi-document isolation.
///
/// Updates to document "alpha" must NOT reach clients connected to "beta",
/// and vice versa.
#[tokio::test]
async fn test_multi_document_isolation() {
    let (addr, _token) = start_server().await;

    let mut alpha1 = YClient::connect(addr, "alpha", 301).await;
    let mut alpha2 = YClient::connect(addr, "alpha", 302).await;
    let mut beta1 = YClient::connect(addr, "beta", 311).await;
    let mut beta2 = YClient::connect(addr, "beta", 312).await;

    // Write to "alpha".
    let upd_a = apply_text_insert(&alpha1.doc, "data", "alpha-value");
    alpha1.send_update(upd_a).await;

    // alpha2 MUST receive the update.
    recv_and_apply_update(&mut alpha2, Duration::from_secs(5)).await;
    assert_eq!(read_text(&alpha2.doc, "data"), "alpha-value");

    // The server echoes the broadcast back to alpha1 (the sender) as well — consume it.
    recv_and_apply_update(&mut alpha1, Duration::from_secs(5)).await;

    // beta1 and beta2 MUST NOT receive any update (500 ms silence).
    let leak1 = beta1.recv_message_timeout(Duration::from_millis(500)).await;
    assert!(
        leak1.is_none(),
        "beta1 received a message that should not have crossed doc boundaries: {:?}",
        leak1
    );
    let leak2 = beta2.recv_message_timeout(Duration::from_millis(500)).await;
    assert!(
        leak2.is_none(),
        "beta2 received a message that should not have crossed doc boundaries: {:?}",
        leak2
    );

    // Write to "beta".
    let upd_b = apply_text_insert(&beta1.doc, "data", "beta-value");
    beta1.send_update(upd_b).await;

    // beta2 MUST receive the update.
    recv_and_apply_update(&mut beta2, Duration::from_secs(5)).await;
    assert_eq!(read_text(&beta2.doc, "data"), "beta-value");

    // The server echoes the broadcast back to beta1 (the sender) as well — consume it.
    recv_and_apply_update(&mut beta1, Duration::from_secs(5)).await;

    // alpha1 and alpha2 MUST NOT receive any update from the "beta" document.
    let leak3 = alpha1
        .recv_message_timeout(Duration::from_millis(500))
        .await;
    assert!(
        leak3.is_none(),
        "alpha1 received a cross-doc leak from beta: {:?}",
        leak3
    );
    let leak4 = alpha2
        .recv_message_timeout(Duration::from_millis(500))
        .await;
    assert!(
        leak4.is_none(),
        "alpha2 received a cross-doc leak from beta: {:?}",
        leak4
    );
}

/// Test 4: Bidirectional concurrent writes with CRDT convergence.
///
/// 4 clients each write to their own named text field.  After all updates
/// propagate, every client's local doc must contain all 4 fields.
#[tokio::test]
async fn test_bidirectional_concurrent_writes() {
    let (addr, _token) = start_server().await;

    let mut clients: Vec<YClient> =
        futures::future::join_all((0u64..4).map(|i| YClient::connect(addr, "doc4", 1000 + i)))
            .await;

    // Each client writes to its own dedicated field.
    for i in 0..4 {
        let field = format!("field_{}", i);
        let content = format!("data-{}", i);
        let update = apply_text_insert(&clients[i].doc, &field, &content);
        clients[i].send_update(update).await;
    }

    // For each client, receive messages until it has all 4 fields populated.
    let timeout = Duration::from_secs(10);
    for i in 0..4 {
        loop {
            let all_present =
                (0..4).all(|j| !read_text(&clients[i].doc, &format!("field_{}", j)).is_empty());
            if all_present {
                break;
            }
            match clients[i].recv_message_timeout(timeout).await {
                Some(YMessage::Sync(YSyncMessage::Update(bytes))) => {
                    let upd = Update::decode_v1(&bytes).expect("decode convergence update");
                    clients[i].doc.transact_mut().apply_update(upd);
                }
                Some(_) => {} // Awareness or other — ignore
                None => panic!("Client {} timed out waiting for CRDT convergence", i),
            }
        }
    }

    // Verify all clients have converged to the same state.
    for i in 0..4 {
        for j in 0..4 {
            assert_eq!(
                read_text(&clients[i].doc, &format!("field_{}", j)),
                format!("data-{}", j),
                "client {} missing field_{} after convergence",
                i,
                j
            );
        }
    }
}

/// Test 5: Awareness broadcast.
///
/// Client A sends an Awareness update → Client B receives it.
#[tokio::test]
async fn test_awareness_broadcast() {
    let (addr, _token) = start_server().await;

    let mut client_a = YClient::connect(addr, "doc5", 501).await;
    let mut client_b = YClient::connect(addr, "doc5", 502).await;

    // Client A broadcasts its cursor position via Awareness.
    client_a
        .send_awareness(r#"{"cursor": 42, "user": "Alice"}"#)
        .await;

    // Client B should receive an Awareness message.
    let msg = client_b
        .recv_message_timeout(Duration::from_secs(5))
        .await
        .expect("Client B timed out waiting for Awareness update");

    assert!(
        matches!(msg, YMessage::Awareness(_)),
        "Expected Awareness message, got {:?}",
        msg
    );
}

/// Test 6: Connect/disconnect resilience.
///
/// 1. Client A connects and writes "A-phase1" to "field_a".
/// 2. Client B connects — handshake delivers existing state; B sees "A-phase1".
/// 3. Client A disconnects.
/// 4. Client B writes "B-phase1" to "field_b".
/// 5. Client C connects — handshake delivers all accumulated state.
/// 6. Client B writes "B-phase2" to "field_c" → Client C receives via broadcast.
#[tokio::test]
async fn test_connect_disconnect_resilience() {
    let (addr, _token) = start_server().await;

    // ── Phase 1: Client A writes ────────────────────────────────────────────
    let mut client_a = YClient::connect(addr, "doc6", 601).await;
    let upd_a1 = apply_text_insert(&client_a.doc, "field_a", "A-phase1");
    client_a.send_update(upd_a1).await;

    // ── Phase 2: Client B connects, gets existing state ─────────────────────
    let mut client_b = YClient::connect(addr, "doc6", 602).await;
    // After the handshake, B's doc already has Client A's "A-phase1".
    assert_eq!(
        read_text(&client_b.doc, "field_a"),
        "A-phase1",
        "Client B should have received A-phase1 during handshake"
    );

    // ── Phase 3: Client A disconnects ───────────────────────────────────────
    client_a.close().await;

    // ── Phase 4: Client B writes ─────────────────────────────────────────────
    let upd_b1 = apply_text_insert(&client_b.doc, "field_b", "B-phase1");
    client_b.send_update(upd_b1).await;

    // ── Phase 5: Client C connects, gets both A's and B's state ─────────────
    let mut client_c = YClient::connect(addr, "doc6", 603).await;
    assert_eq!(
        read_text(&client_c.doc, "field_a"),
        "A-phase1",
        "Client C should have A-phase1 from handshake"
    );
    assert_eq!(
        read_text(&client_c.doc, "field_b"),
        "B-phase1",
        "Client C should have B-phase1 from handshake"
    );

    // ── Phase 6: Client B writes, Client C receives via broadcast ────────────
    let upd_b2 = apply_text_insert(&client_b.doc, "field_c", "B-phase2");
    client_b.send_update(upd_b2).await;

    recv_and_apply_update(&mut client_c, Duration::from_secs(5)).await;
    assert_eq!(
        read_text(&client_c.doc, "field_c"),
        "B-phase2",
        "Client C should have received B-phase2 via broadcast"
    );
}
