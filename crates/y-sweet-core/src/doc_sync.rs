#[cfg(feature = "sync")]
use crate::sync::{Message, SyncMessage};
use crate::{doc_connection::DOC_NAME, store::Store, sync::awareness::Awareness, sync_kv::SyncKv};
use anyhow::{anyhow, Context, Result};
use std::sync::{atomic::AtomicUsize, Arc, RwLock};
#[cfg(feature = "sync")]
use tokio::sync::broadcast;
#[cfg(feature = "sync")]
use yrs::updates::encoder::Encode;
use yrs::{updates::decoder::Decode, ReadTxn, StateVector, Subscription, Transact, Update};
use yrs_kvstore::DocOps;

pub struct DocWithSyncKv {
    awareness: Arc<RwLock<Awareness>>,
    sync_kv: Arc<SyncKv>,
    #[allow(unused)] // acts as RAII guard
    subscription: Subscription,
    connection_count: Arc<AtomicUsize>,
    /// RAII guard for the single awareness observer that fans out awareness
    /// updates over `awareness_update_tx`. Only registered for the native
    /// (multi-threaded) server.
    #[cfg(feature = "sync")]
    #[allow(unused)]
    awareness_subscription: Subscription,
    /// Broadcast channel for raw CRDT update bytes, pre-encoded as MSG_SYNC_UPDATE.
    /// Receivers forward the bytes directly to WebSocket clients. This replaces
    /// per-connection observers, reducing write-lock hold time from O(N) to O(1).
    #[cfg(feature = "sync")]
    doc_update_tx: broadcast::Sender<Vec<u8>>,
    /// Broadcast channel for pre-encoded awareness update messages.
    #[cfg(feature = "sync")]
    awareness_update_tx: broadcast::Sender<Vec<u8>>,
}

impl DocWithSyncKv {
    pub fn awareness(&self) -> Arc<RwLock<Awareness>> {
        self.awareness.clone()
    }

    pub fn sync_kv(&self) -> Arc<SyncKv> {
        self.sync_kv.clone()
    }

    pub fn connection_count(&self) -> Arc<AtomicUsize> {
        self.connection_count.clone()
    }

    /// Subscribe to receive pre-encoded MSG_SYNC_UPDATE messages whenever the
    /// document changes. Receivers can forward the bytes directly to WebSocket
    /// clients. On `RecvError::Lagged`, the receiver should perform a full
    /// re-sync via SyncStep2.
    #[cfg(feature = "sync")]
    pub fn subscribe_doc_updates(&self) -> broadcast::Receiver<Vec<u8>> {
        self.doc_update_tx.subscribe()
    }

    /// Subscribe to receive pre-encoded awareness update messages whenever
    /// awareness changes.
    #[cfg(feature = "sync")]
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

        // Broadcast channel capacity: 1024 updates before slow receivers get
        // Lagged. When Lagged occurs, receivers perform a full re-sync via
        // SyncStep2.
        #[cfg(feature = "sync")]
        let (doc_update_tx, _) = broadcast::channel::<Vec<u8>>(1024);
        #[cfg(feature = "sync")]
        let (awareness_update_tx, _) = broadcast::channel::<Vec<u8>>(1024);

        // Single document observer: persists every update to SyncKv and, on the
        // native server, also broadcasts the pre-encoded MSG_SYNC_UPDATE message
        // to all connections. Pre-encoding once keeps the write-lock hold time
        // O(1) regardless of the number of connected clients.
        let subscription = {
            let sync_kv = sync_kv.clone();
            #[cfg(feature = "sync")]
            let tx = doc_update_tx.clone();
            doc.observe_update_v1(move |_, event| {
                sync_kv.push_update(DOC_NAME, &event.update).unwrap();
                sync_kv
                    .flush_doc_with(DOC_NAME, Default::default())
                    .unwrap();
                #[cfg(feature = "sync")]
                {
                    let encoded =
                        Message::Sync(SyncMessage::Update(event.update.to_vec())).encode_v1();
                    // Ignore errors when no receivers are subscribed.
                    let _ = tx.send(encoded);
                }
            })
            .map_err(|_| anyhow!("Failed to subscribe to updates"))?
        };

        let awareness = Arc::new(RwLock::new(Awareness::new(doc)));

        // Single awareness observer (native server only): broadcasts pre-encoded
        // awareness update messages. On the wasm worker, awareness fan-out stays
        // in per-connection observers inside DocConnection.
        #[cfg(feature = "sync")]
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
            subscription,
            connection_count: Arc::new(AtomicUsize::new(0)),
            #[cfg(feature = "sync")]
            awareness_subscription,
            #[cfg(feature = "sync")]
            doc_update_tx,
            #[cfg(feature = "sync")]
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
