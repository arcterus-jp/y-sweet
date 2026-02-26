use crate::api_types::Authorization;
use crate::sync::{self, awareness::Awareness, DefaultProtocol, Message, Protocol, SyncMessage};
use std::sync::{Arc, OnceLock, RwLock};
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
/// Fan-out of document and awareness updates is handled via `tokio::sync::broadcast`
/// channels in `DocWithSyncKv`, not via per-connection observers. This reduces write-lock
/// hold time from O(N) to O(1) when N clients are connected to the same document.
pub struct DocConnection {
    awareness: Arc<RwLock<Awareness>>,
    authorization: Authorization,
    callback: Callback,

    /// If the client sends an awareness state, this will be set to its client ID.
    /// It is used to clear the awareness state when a client disconnects.
    client_id: OnceLock<ClientID>,
}

impl DocConnection {
    #[cfg(not(feature = "sync"))]
    pub fn new<F>(
        awareness: Arc<RwLock<Awareness>>,
        authorization: Authorization,
        callback: F,
    ) -> Self
    where
        F: Fn(&[u8]) + 'static,
    {
        Self::new_inner(awareness, authorization, Arc::new(callback))
    }

    #[cfg(feature = "sync")]
    pub fn new<F>(
        awareness: Arc<RwLock<Awareness>>,
        authorization: Authorization,
        callback: F,
    ) -> Self
    where
        F: Fn(&[u8]) + 'static + Send + Sync,
    {
        Self::new_inner(awareness, authorization, Arc::new(callback))
    }

    pub fn new_inner(
        awareness: Arc<RwLock<Awareness>>,
        authorization: Authorization,
        callback: Callback,
    ) -> Self {
        // Use a read lock for the initial handshake — no observer registration needed here.
        // Document and awareness update fan-out is handled via broadcast channels in
        // DocWithSyncKv, so no per-connection observers are registered.
        {
            let awareness = awareness.read().unwrap();

            // Initial handshake based on:
            // https://github.com/y-crdt/y-sync/blob/56958e83acfd1f3c09f5dd67cf23c9c72f000707/src/sync.rs#L45-L54

            // Send server-side state vector so the client can send offline updates.
            let sv = awareness.doc().transact().state_vector();
            let sync_step_1 = Message::Sync(SyncMessage::SyncStep1(sv)).encode_v1();
            callback(&sync_step_1);

            // Send initial awareness state.
            let update = awareness.update().unwrap();
            let awareness_msg = Message::Awareness(update).encode_v1();
            callback(&awareness_msg);
        }

        Self {
            awareness,
            authorization,
            callback,
            client_id: OnceLock::new(),
        }
    }

    pub async fn send(&self, update: &[u8]) -> Result<(), anyhow::Error> {
        let msg = Message::decode_v1(update)?;
        let result = self.handle_msg(&DefaultProtocol, msg)?;

        if let Some(result) = result {
            let msg = result.encode_v1();
            (self.callback)(&msg);
        }

        Ok(())
    }

    /// Process a batch of incoming messages, applying all CRDT updates in a single
    /// write-lock acquisition to minimize lock contention under burst load.
    ///
    /// All `SyncMessage::Update` messages are collected and applied within one
    /// `transact_mut()` call, causing the doc observer to fire only once per batch.
    /// Non-Update messages (SyncStep1, Auth, Awareness, etc.) are processed individually
    /// after the batched updates. Responses are sent via the connection callback.
    pub async fn send_batch(&self, updates: &[Vec<u8>]) -> Result<(), anyhow::Error> {
        if updates.is_empty() {
            return Ok(());
        }

        if updates.len() == 1 {
            return self.send(&updates[0]).await;
        }

        let can_write = matches!(self.authorization, Authorization::Full);

        // Separate CRDT update messages from other message types.
        let mut other_indices: Vec<usize> = Vec::new();
        let mut has_crdt_updates = false;

        for (i, raw) in updates.iter().enumerate() {
            match Message::decode_v1(raw)? {
                Message::Sync(SyncMessage::Update(_)) if can_write => {
                    has_crdt_updates = true;
                }
                _ => {
                    other_indices.push(i);
                }
            }
        }

        // Apply all CRDT updates in a single transaction to trigger the observer once.
        // When the transaction is committed (dropped), the single broadcast observer fires
        // exactly once, regardless of how many updates were batched.
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
            // Transaction commits (and observer fires once) when txn is dropped here.
        }

        // Process all non-Update messages individually.
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
        // If this client had an awareness state, remove it.
        if let Some(client_id) = self.client_id.get() {
            let mut awareness = self.awareness.write().unwrap();
            awareness.remove_state(*client_id);
        }
    }
}
