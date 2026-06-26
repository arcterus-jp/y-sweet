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

    /// Process a batch of incoming messages, applying all CRDT updates in a
    /// single write-lock acquisition to minimize lock contention under burst
    /// load.
    ///
    /// All `SyncMessage::Update` messages are collected and applied within one
    /// `transact_mut()` call, so the broadcast observer in `DocWithSyncKv` fires
    /// only once per batch. Non-Update messages (SyncStep1, SyncStep2, Auth,
    /// Awareness, etc.) are processed individually, in order, after the batched
    /// updates. Responses are sent via the connection callback.
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

        // Identify CRDT update messages so they can share one transaction, and
        // remember the order of all other messages for individual processing.
        let mut other_indices: Vec<usize> = Vec::new();
        let mut has_crdt_updates = false;

        for (i, raw) in updates.iter().enumerate() {
            match Message::decode_v1(raw)? {
                Message::Sync(SyncMessage::Update(_)) if can_write => {
                    has_crdt_updates = true;
                }
                _ => other_indices.push(i),
            }
        }

        // Apply all CRDT updates in a single transaction. The broadcast observer
        // fires exactly once when the transaction is committed (on drop),
        // regardless of how many updates were batched.
        if has_crdt_updates {
            let awareness = self.awareness.write().unwrap();
            let mut txn = awareness.doc().transact_mut();
            for raw in updates {
                if let Ok(Message::Sync(SyncMessage::Update(u))) = Message::decode_v1(raw) {
                    if let Ok(update) = Update::decode_v1(&u) {
                        txn.apply_update(update);
                    }
                }
            }
            // Transaction commits (and the observer fires once) when txn drops.
        }

        // Process all non-Update messages individually, preserving their order.
        for &i in &other_indices {
            let msg = Message::decode_v1(&updates[i])?;
            if let Some(result) = self.handle_msg(&DefaultProtocol, msg)? {
                let msg = result.encode_v1();
                (self.callback)(&msg);
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
