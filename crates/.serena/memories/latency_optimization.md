# High-Load Latency Optimization (Broadcast Fan-out)

## Problem
Under high concurrent connections to the same document, latency degraded due to:
- `Arc<RwLock<Awareness>>` with O(N) observer callbacks fired while write lock is held
- Each of N DocConnections registered its own `observe_update_v1` and `awareness.on_update`
- Mutex-under-RwLock nesting for SyncKv persistence

## Solution (Phase 1+2+3)

### Phase 1+2: Broadcast channels in DocWithSyncKv (doc_sync.rs)
- Added `doc_update_tx: broadcast::Sender<Vec<u8>>` and `awareness_update_tx: broadcast::Sender<Vec<u8>>`
- Single `observe_update_v1` observer: persists to SyncKv + broadcasts pre-encoded MSG_SYNC_UPDATE
- Single `awareness.on_update` observer: broadcasts pre-encoded awareness update
- Lock hold time reduced from O(N) to O(1)

### Phase 2: Remove per-connection observers from DocConnection (doc_connection.rs)
- Removed `doc_subscription` and `awareness_subscription` fields
- Initial handshake now uses READ lock instead of WRITE lock
- Removed `closed: Arc<OnceLock<()>>` field (no longer needed without observers)
- Added `send_batch()` method for batched CRDT update processing

### Phase 3: Batch processing in handle_socket (server.rs)
- Forward task: reads WebSocket stream, handles Pong/Close, forwards binary to mpsc channel
- Sender task: handles 3 sources: callback replies, doc broadcast, awareness broadcast
- Batch loop: collects available messages via `try_recv`, applies in single `send_batch` call
- Lagged broadcast receivers trigger full re-sync via SyncStep2

## Key Files Changed
- `y-sweet-core/Cargo.toml`: Added `tokio = { features = ["sync"] }`
- `y-sweet/Cargo.toml`: Added `sync` feature to tokio
- `y-sweet-core/src/doc_sync.rs`: Broadcast channels, single observers
- `y-sweet-core/src/doc_connection.rs`: No observers, read lock for handshake, send_batch
- `y-sweet/src/server.rs`: New handle_socket with broadcast receivers + batch processing

## Performance Impact
- Write lock hold time: O(N) → O(1) per update
- N concurrent writers: O(N²) → O(N) total throughput
- Expected capacity: 80-100 → 300-500 connections per document
