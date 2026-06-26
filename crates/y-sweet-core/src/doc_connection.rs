use crate::api_types::Authorization;
use crate::sync::{self, awareness::Awareness, DefaultProtocol, Message, Protocol, SyncMessage};
#[cfg(not(feature = "sync"))]
use crate::sync::{MSG_SYNC, MSG_SYNC_UPDATE};
use std::sync::{Arc, OnceLock, RwLock};
#[cfg(not(feature = "sync"))]
use yrs::encoding::write::Write;
#[cfg(not(feature = "sync"))]
use yrs::updates::encoder::{Encoder, EncoderV1};
#[cfg(not(feature = "sync"))]
use yrs::Subscription;
use yrs::{
    block::ClientID,
    updates::{decoder::Decode, encoder::Encode},
    ReadTxn, Transact, Update,
};

// TODO: this is an implementation detail and should not be exposed.
pub const DOC_NAME: &str = "doc";

#[cfg(not(feature = "sync"))]
type Callback = Arc<dyn Fn(&[u8]) + 'static>;

#[cfg(feature = "sync")]
type Callback = Arc<dyn Fn(&[u8]) + 'static + Send + Sync>;

const SYNC_STATUS_MESSAGE: u8 = 102;

/// A connection to a Yjs document over the y-sync protocol.
///
/// On the native (multi-threaded) server, fan-out of document and awareness
/// updates is handled via `tokio::sync::broadcast` channels in `DocWithSyncKv`,
/// not via per-connection observers. This reduces write-lock hold time from
/// O(N) to O(1) when N clients are connected to the same document. On the wasm
/// worker (`not(feature = "sync")`), per-connection observers are retained.
pub struct DocConnection {
    awareness: Arc<RwLock<Awareness>>,
    #[cfg(not(feature = "sync"))]
    #[allow(unused)] // acts as RAII guard
    doc_subscription: Subscription,
    #[cfg(not(feature = "sync"))]
    #[allow(unused)] // acts as RAII guard
    awareness_subscription: Subscription,
    authorization: Authorization,
    callback: Callback,
    #[cfg(not(feature = "sync"))]
    closed: Arc<OnceLock<()>>,
    doc_id: String,

    /// If the client sends an awareness state, this will be set to its client ID.
    /// It is used to clear the awareness state when a client disconnects.
    client_id: OnceLock<ClientID>,
}

impl DocConnection {
    #[cfg(not(feature = "sync"))]
    pub fn new<F>(
        doc_id: String,
        awareness: Arc<RwLock<Awareness>>,
        authorization: Authorization,
        callback: F,
    ) -> Self
    where
        F: Fn(&[u8]) + 'static,
    {
        Self::new_inner(doc_id, awareness, authorization, Arc::new(callback))
    }

    #[cfg(feature = "sync")]
    pub fn new<F>(
        doc_id: String,
        awareness: Arc<RwLock<Awareness>>,
        authorization: Authorization,
        callback: F,
    ) -> Self
    where
        F: Fn(&[u8]) + 'static + Send + Sync,
    {
        Self::new_inner(doc_id, awareness, authorization, Arc::new(callback))
    }

    pub fn new_inner(
        doc_id: String,
        awareness: Arc<RwLock<Awareness>>,
        authorization: Authorization,
        callback: Callback,
    ) -> Self {
        #[cfg(not(feature = "sync"))]
        let closed = Arc::new(OnceLock::new());

        // Native server: read-lock handshake only. Document and awareness
        // fan-out is handled by broadcast channels in DocWithSyncKv, so no
        // per-connection observers are registered here.
        #[cfg(feature = "sync")]
        {
            let awareness = awareness.read().unwrap();

            // Initial handshake is based on this:
            // https://github.com/y-crdt/y-sync/blob/56958e83acfd1f3c09f5dd67cf23c9c72f000707/src/sync.rs#L45-L54
            let span = tracing::info_span!("ws.initial_sync", doc_id = %doc_id);
            let _guard = span.enter();

            // Send a server-side state vector, so that the client can send
            // updates that happened offline.
            let sv = awareness.doc().transact().state_vector();
            let sync_step_1 = Message::Sync(SyncMessage::SyncStep1(sv)).encode_v1();
            callback(&sync_step_1);

            // Send the initial awareness state.
            let update = awareness.update().unwrap();
            let awareness_msg = Message::Awareness(update).encode_v1();
            callback(&awareness_msg);
        }

        // Wasm worker: register per-connection observers for fan-out, since the
        // broadcast channels (which require tokio) are not compiled in.
        #[cfg(not(feature = "sync"))]
        let (doc_subscription, awareness_subscription) = {
            let mut awareness = awareness.write().unwrap();

            {
                let span = tracing::info_span!("ws.initial_sync", doc_id = %doc_id);
                let _guard = span.enter();

                {
                    // Send a server-side state vector, so that the client can send
                    // updates that happened offline.
                    let sv = awareness.doc().transact().state_vector();
                    let sync_step_1 = Message::Sync(SyncMessage::SyncStep1(sv)).encode_v1();
                    callback(&sync_step_1);
                }

                {
                    // Send the initial awareness state.
                    let update = awareness.update().unwrap();
                    let awareness = Message::Awareness(update).encode_v1();
                    callback(&awareness);
                }
            }

            let doc_subscription = {
                let doc = awareness.doc();
                let callback = callback.clone();
                let closed = closed.clone();
                doc.observe_update_v1(move |_, event| {
                    if closed.get().is_some() {
                        return;
                    }
                    // https://github.com/y-crdt/y-sync/blob/56958e83acfd1f3c09f5dd67cf23c9c72f000707/src/net/broadcast.rs#L47-L52
                    let mut encoder = EncoderV1::new();
                    encoder.write_var(MSG_SYNC);
                    encoder.write_var(MSG_SYNC_UPDATE);
                    encoder.write_buf(&event.update);
                    let msg = encoder.to_vec();
                    callback(&msg);
                })
                .unwrap()
            };

            let callback = callback.clone();
            let closed = closed.clone();
            let awareness_subscription = awareness.on_update(move |awareness, e| {
                if closed.get().is_some() {
                    return;
                }

                // https://github.com/y-crdt/y-sync/blob/56958e83acfd1f3c09f5dd67cf23c9c72f000707/src/net/broadcast.rs#L59
                let added = e.added();
                let updated = e.updated();
                let removed = e.removed();
                let mut changed = Vec::with_capacity(added.len() + updated.len() + removed.len());
                changed.extend_from_slice(added);
                changed.extend_from_slice(updated);
                changed.extend_from_slice(removed);

                if let Ok(u) = awareness.update_with_clients(changed) {
                    let msg = Message::Awareness(u).encode_v1();
                    callback(&msg);
                }
            });

            (doc_subscription, awareness_subscription)
        };

        Self {
            awareness,
            #[cfg(not(feature = "sync"))]
            doc_subscription,
            #[cfg(not(feature = "sync"))]
            awareness_subscription,
            authorization,
            callback,
            client_id: OnceLock::new(),
            #[cfg(not(feature = "sync"))]
            closed,
            doc_id,
        }
    }

    pub async fn send(&self, update: &[u8]) -> Result<(), anyhow::Error> {
        let span = tracing::info_span!(
            "ws.message.process",
            doc_id = %self.doc_id,
            message_type = tracing::field::Empty,
            payload_size = update.len(),
        );
        let _guard = span.enter();

        let msg = Message::decode_v1(update)?;

        span.record(
            "message_type",
            match &msg {
                Message::Sync(SyncMessage::SyncStep1(_)) => "sync_step1",
                Message::Sync(SyncMessage::SyncStep2(_)) => "sync_step2",
                Message::Sync(SyncMessage::Update(_)) => "sync_update",
                Message::Auth(_) => "auth",
                Message::AwarenessQuery => "awareness_query",
                Message::Awareness(_) => "awareness",
                Message::Custom(_, _) => "custom",
            },
        );

        let result = self.handle_msg(&DefaultProtocol, msg)?;

        if let Some(result) = result {
            let msg = result.encode_v1();
            (self.callback)(&msg);
        }

        Ok(())
    }

    /// Process a batch of incoming messages, applying all writable CRDT updates
    /// in a single write-lock acquisition to minimize lock contention under
    /// burst load.
    ///
    /// Writable `SyncMessage::Update` messages are collected and applied within
    /// one `transact_mut()` call, so the broadcast observer in `DocWithSyncKv`
    /// fires only once per batch. All other messages (SyncStep1, SyncStep2,
    /// Auth, Awareness, and — for read-only connections — Update) are processed
    /// individually, in order, after the batched updates, with responses sent
    /// via the connection callback.
    ///
    /// Per-message errors (e.g. a read-only client attempting a write, or an
    /// undecodable frame) are logged and skipped rather than aborting the whole
    /// batch — mirroring the single-message [`send`](Self::send) path, which the
    /// caller invokes one message at a time.
    #[cfg(feature = "sync")]
    pub async fn send_batch(&self, updates: &[Vec<u8>]) -> Result<(), anyhow::Error> {
        if updates.is_empty() {
            return Ok(());
        }

        // Single-message fast path keeps per-message tracing for the common case.
        if updates.len() == 1 {
            return self.send(&updates[0]).await;
        }

        let span = tracing::info_span!(
            "ws.message.batch",
            doc_id = %self.doc_id,
            batch_size = updates.len(),
        );
        let _guard = span.enter();

        let can_write = matches!(self.authorization, Authorization::Full);

        // Decode each frame once. Writable CRDT updates are set aside to share a
        // single transaction; everything else keeps its original order for
        // individual processing. Undecodable frames are logged and skipped.
        let mut writable_updates: Vec<Vec<u8>> = Vec::new();
        let mut others: Vec<Message> = Vec::new();
        for raw in updates {
            match Message::decode_v1(raw) {
                Ok(Message::Sync(SyncMessage::Update(u))) if can_write => {
                    writable_updates.push(u);
                }
                Ok(msg) => others.push(msg),
                Err(e) => {
                    tracing::warn!(
                        message = "Failed to decode batched message",
                        event = "websocket_batch_decode_error",
                        error = %e,
                    );
                }
            }
        }

        // Apply all writable CRDT updates in a single transaction. The broadcast
        // observer fires exactly once when the transaction is committed (on
        // drop), regardless of how many updates were batched.
        if !writable_updates.is_empty() {
            let awareness = self.awareness.write().unwrap();
            let mut txn = awareness.doc().transact_mut();
            for u in &writable_updates {
                if let Ok(update) = Update::decode_v1(u) {
                    txn.apply_update(update);
                }
            }
            // Transaction commits (and the observer fires once) when txn drops.
        }

        // Process the remaining messages in order. A per-message error here is
        // expected (e.g. read-only write attempts return PermissionDenied) and
        // must not abort the rest of the batch — notably the SyncStep1 that
        // follows a rejected SyncStep2 during the read-only handshake.
        for msg in others {
            match self.handle_msg(&DefaultProtocol, msg) {
                Ok(Some(result)) => (self.callback)(&result.encode_v1()),
                Ok(None) => {}
                Err(e) => {
                    tracing::warn!(
                        message = "Batched message handling error",
                        event = "websocket_batch_message_error",
                        error = %e,
                    );
                }
            }
        }

        Ok(())
    }

    // Adapted from:
    // https://github.com/y-crdt/y-sync/blob/56958e83acfd1f3c09f5dd67cf23c9c72f000707/src/net/conn.rs#L184C1-L222C1
    pub fn handle_msg<P: Protocol>(
        &self,
        protocol: &P,
        msg: Message,
    ) -> Result<Option<Message>, sync::Error> {
        let can_write = matches!(self.authorization, Authorization::Full);
        let a = &self.awareness;
        match msg {
            Message::Sync(msg) => match msg {
                SyncMessage::SyncStep1(sv) => {
                    let awareness = a.read().unwrap();
                    protocol.handle_sync_step1(&awareness, sv)
                }
                SyncMessage::SyncStep2(update) => {
                    if can_write {
                        let mut awareness = a.write().unwrap();
                        protocol.handle_sync_step2(&mut awareness, Update::decode_v1(&update)?)
                    } else {
                        Err(sync::Error::PermissionDenied {
                            reason: "Token does not have write access".to_string(),
                        })
                    }
                }
                SyncMessage::Update(update) => {
                    if can_write {
                        let mut awareness = a.write().unwrap();
                        protocol.handle_update(&mut awareness, Update::decode_v1(&update)?)
                    } else {
                        Err(sync::Error::PermissionDenied {
                            reason: "Token does not have write access".to_string(),
                        })
                    }
                }
            },
            Message::Auth(reason) => {
                let awareness = a.read().unwrap();
                protocol.handle_auth(&awareness, reason)
            }
            Message::AwarenessQuery => {
                let awareness = a.read().unwrap();
                protocol.handle_awareness_query(&awareness)
            }
            Message::Awareness(update) => {
                if update.clients.len() == 1 {
                    let client_id = update.clients.keys().next().unwrap();
                    self.client_id.get_or_init(|| *client_id);
                } else {
                    tracing::warn!("Received awareness update with more than one client");
                }
                let mut awareness = a.write().unwrap();
                protocol.handle_awareness_update(&mut awareness, update)
            }
            Message::Custom(SYNC_STATUS_MESSAGE, data) => {
                // Respond to the client with the same payload it sent.
                Ok(Some(Message::Custom(SYNC_STATUS_MESSAGE, data)))
            }
            Message::Custom(tag, data) => {
                let mut awareness = a.write().unwrap();
                protocol.missing_handle(&mut awareness, tag, data)
            }
        }
    }
}

impl Drop for DocConnection {
    fn drop(&mut self) {
        #[cfg(not(feature = "sync"))]
        self.closed.set(()).unwrap();

        // If this client had an awareness state, remove it.
        if let Some(client_id) = self.client_id.get() {
            let mut awareness = self.awareness.write().unwrap();
            awareness.remove_state(*client_id);
        }
    }
}

#[cfg(all(test, feature = "sync"))]
mod tests {
    use super::*;
    use crate::sync::awareness::Awareness;
    use std::sync::Mutex;
    use yrs::{Doc, GetString, Text, Transact};

    /// Collects every message the connection emits via its callback.
    fn collecting_connection(
        authorization: Authorization,
    ) -> (DocConnection, Arc<Mutex<Vec<Vec<u8>>>>) {
        let doc = Doc::new();
        let awareness = Arc::new(RwLock::new(Awareness::new(doc)));
        let sink = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
        let sink_cb = sink.clone();
        let connection = DocConnection::new(
            "test-doc".to_string(),
            awareness,
            authorization,
            move |bytes| sink_cb.lock().unwrap().push(bytes.to_vec()),
        );
        (connection, sink)
    }

    /// Encode a SyncStep2 carrying the full state of a doc that contains some text.
    fn sync_step2_with_text(text: &str) -> Vec<u8> {
        let doc = Doc::new();
        let txt = doc.get_or_insert_text("content");
        txt.push(&mut doc.transact_mut(), text);
        let update = doc
            .transact()
            .encode_state_as_update_v1(&yrs::StateVector::default());
        Message::Sync(SyncMessage::SyncStep2(update)).encode_v1()
    }

    fn sync_step1_empty() -> Vec<u8> {
        Message::Sync(SyncMessage::SyncStep1(yrs::StateVector::default())).encode_v1()
    }

    /// Regression test: a read-only client's handshake batches its
    /// `[SyncStep2, SyncStep1]` together. The rejected SyncStep2
    /// (PermissionDenied) must NOT abort the batch — the server still has to
    /// answer the trailing SyncStep1 with its own SyncStep2, or the client never
    /// completes sync. See the `read-only over websocket` integration test.
    #[tokio::test]
    async fn read_only_batch_does_not_drop_sync_step1_response() {
        let (connection, sink) = collecting_connection(Authorization::ReadOnly);
        // Discard the handshake messages emitted by `new`.
        sink.lock().unwrap().clear();

        let batch = vec![sync_step2_with_text("hello"), sync_step1_empty()];
        connection.send_batch(&batch).await.unwrap();

        let responses = sink.lock().unwrap().clone();
        let got_step2 = responses.iter().any(|raw| {
            matches!(
                Message::decode_v1(raw),
                Ok(Message::Sync(SyncMessage::SyncStep2(_)))
            )
        });
        assert!(
            got_step2,
            "read-only batch should still answer SyncStep1 with a SyncStep2 (got {} messages)",
            responses.len()
        );
    }

    /// A writable client batching several `Update`s applies them all in one
    /// transaction, and a trailing SyncStep1 is answered with the post-update
    /// state.
    #[tokio::test]
    async fn writable_batch_applies_updates_and_answers_sync_step1() {
        let (connection, sink) = collecting_connection(Authorization::Full);
        sink.lock().unwrap().clear();

        // Two updates to the same text field, plus a SyncStep1 asking for state.
        let mut batch = Vec::new();
        for chunk in ["foo", "bar"] {
            let doc = Doc::new();
            let txt = doc.get_or_insert_text("content");
            txt.push(&mut doc.transact_mut(), chunk);
            let update = doc
                .transact()
                .encode_state_as_update_v1(&yrs::StateVector::default());
            batch.push(Message::Sync(SyncMessage::Update(update)).encode_v1());
        }
        batch.push(sync_step1_empty());

        connection.send_batch(&batch).await.unwrap();

        // The connection's awareness doc must reflect both updates.
        let merged = {
            let awareness = connection.awareness.read().unwrap();
            let doc = awareness.doc();
            let txt = doc.get_or_insert_text("content");
            let txn = doc.transact();
            let s = txt.get_string(&txn);
            drop(txn);
            s
        };
        assert!(
            merged.contains("foo") && merged.contains("bar"),
            "batched updates were not all applied: {merged:?}"
        );

        // And the trailing SyncStep1 produced a SyncStep2 response.
        let responses = sink.lock().unwrap().clone();
        assert!(
            responses.iter().any(|raw| matches!(
                Message::decode_v1(raw),
                Ok(Message::Sync(SyncMessage::SyncStep2(_)))
            )),
            "writable batch should answer SyncStep1 with a SyncStep2"
        );
    }
}
