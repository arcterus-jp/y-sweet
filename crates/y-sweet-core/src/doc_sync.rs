use crate::{
    doc_connection::DOC_NAME,
    store::Store,
    sync::{awareness::Awareness, Message, SyncMessage},
    sync_kv::SyncKv,
};
use anyhow::{anyhow, Context, Result};
use std::sync::{Arc, RwLock};
use tokio::sync::broadcast;
use yrs::{
    updates::{decoder::Decode, encoder::Encode},
    ReadTxn, StateVector, Subscription, Transact, Update,
};
use yrs_kvstore::DocOps;

pub struct DocWithSyncKv {
    awareness: Arc<RwLock<Awareness>>,
    sync_kv: Arc<SyncKv>,
    #[allow(unused)] // acts as RAII guard
    doc_subscription: Subscription,
    #[allow(unused)] // acts as RAII guard
    awareness_subscription: Subscription,
    /// Broadcast channel for raw CRDT update bytes.
    /// Receivers encode these as MSG_SYNC_UPDATE before sending to clients.
    doc_update_tx: broadcast::Sender<Vec<u8>>,
    /// Broadcast channel for pre-encoded awareness update messages.
    awareness_update_tx: broadcast::Sender<Vec<u8>>,
}

impl DocWithSyncKv {
    pub fn awareness(&self) -> Arc<RwLock<Awareness>> {
        self.awareness.clone()
    }

    pub fn sync_kv(&self) -> Arc<SyncKv> {
        self.sync_kv.clone()
    }

    /// Subscribe to receive pre-encoded MSG_SYNC_UPDATE messages whenever the document changes.
    /// Receivers can forward the bytes directly to WebSocket clients.
    /// On `RecvError::Lagged`, perform a full re-sync via SyncStep2.
    pub fn subscribe_doc_updates(&self) -> broadcast::Receiver<Vec<u8>> {
        self.doc_update_tx.subscribe()
    }

    /// Subscribe to receive pre-encoded awareness update messages whenever awareness changes.
    pub fn subscribe_awareness_updates(&self) -> broadcast::Receiver<Vec<u8>> {
        self.awareness_update_tx.subscribe()
    }

    pub async fn new<F>(
        key: &str,
        store: Option<Arc<Box<dyn Store>>>,
        dirty_callback: F,
        skip_gc: bool,
    ) -> Result<Self>
    where
        F: Fn() + Send + Sync + 'static,
    {
        let sync_kv = SyncKv::new(store, key, dirty_callback)
            .await
            .context("Failed to create SyncKv")?;

        let sync_kv = Arc::new(sync_kv);
        let doc = yrs::Doc::with_options(yrs::Options {
            skip_gc,
            ..yrs::Options::default()
        });

        {
            let mut txn = doc.transact_mut();
            tracing::debug!("Attempting to load existing document data for {}", key);

            match sync_kv.load_doc(DOC_NAME, &mut txn) {
                Ok(result) => {
                    tracing::debug!("Successfully loaded document data, result: {:?}", result);
                }
                Err(e) => {
                    tracing::error!("Failed to load document data: {:?}", e);
                    return Err(anyhow!("Failed to load doc: {:?}", e));
                }
            }
        }

        // Broadcast channel capacity: 1024 updates before slow receivers get Lagged.
        // When Lagged occurs, receivers perform a full re-sync via SyncStep2.
        let (doc_update_tx, _) = broadcast::channel::<Vec<u8>>(1024);
        let (awareness_update_tx, _) = broadcast::channel::<Vec<u8>>(1024);

        // Single observer: persists the update AND broadcasts the fully-encoded
        // MSG_SYNC_UPDATE message to all connections.
        //
        // This replaces per-connection observers in DocConnection, reducing lock hold time
        // from O(N) to O(1) when N clients are connected. Each broadcast receiver just
        // forwards the pre-encoded bytes directly to its WebSocket sink.
        let doc_subscription = {
            let sync_kv = sync_kv.clone();
            let tx = doc_update_tx.clone();
            doc.observe_update_v1(move |_, event| {
                // Persist update to SyncKv (in-memory BTreeMap operation)
                sync_kv.push_update(DOC_NAME, &event.update).unwrap();
                sync_kv
                    .flush_doc_with(DOC_NAME, Default::default())
                    .unwrap();
                // Pre-encode as MSG_SYNC_UPDATE so each receiver can forward directly.
                // O(1) regardless of receiver count.
                let encoded = Message::Sync(SyncMessage::Update(event.update.to_vec())).encode_v1();
                // Ignore errors when no receivers are subscribed.
                let _ = tx.send(encoded);
            })
            .map_err(|_| anyhow!("Failed to subscribe to doc updates"))?
        };

        let awareness = Arc::new(RwLock::new(Awareness::new(doc)));

        // Single awareness observer: broadcasts pre-encoded awareness update messages.
        let awareness_subscription = {
            let tx = awareness_update_tx.clone();
            awareness.write().unwrap().on_update(move |awareness, e| {
                let added = e.added();
                let updated = e.updated();
                let removed = e.removed();
                let mut changed = Vec::with_capacity(added.len() + updated.len() + removed.len());
                changed.extend_from_slice(added);
                changed.extend_from_slice(updated);
                changed.extend_from_slice(removed);

                if let Ok(u) = awareness.update_with_clients(changed) {
                    let msg = Message::Awareness(u).encode_v1();
                    // Ignore errors when no receivers are subscribed.
                    let _ = tx.send(msg);
                }
            })
        };

        Ok(Self {
            awareness,
            sync_kv,
            doc_subscription,
            awareness_subscription,
            doc_update_tx,
            awareness_update_tx,
        })
    }

    pub fn as_update(&self) -> Vec<u8> {
        let awareness_guard = self.awareness.read().unwrap();
        let doc = &awareness_guard.doc;

        let txn = doc.transact();

        txn.encode_state_as_update_v1(&StateVector::default())
    }

    pub fn apply_update(&self, update: &[u8]) -> Result<()> {
        let awareness_guard = self.awareness.write().unwrap();
        let doc = &awareness_guard.doc;

        let update: Update =
            Update::decode_v1(update).map_err(|_| anyhow!("Failed to decode update"))?;

        let mut txn = doc.transact_mut();
        txn.apply_update(update);

        Ok(())
    }
}
