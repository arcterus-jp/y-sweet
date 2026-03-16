# File Modification Rules

## DO NOT Modify (Upstream Compatible)

These files must remain compatible with upstream. **Do not modify without explicit user permission:**

### Core Files
- `y-sweet-core/src/auth.rs` - Authentication logic
- `y-sweet-core/src/doc_connection.rs` - Document connection handling
- `y-sweet-core/src/doc_sync.rs` - Document synchronization
- `y-sweet-core/src/sync/` - CRDT sync logic (entire directory)

### Server Files
- `y-sweet/src/cli.rs` - CLI command definitions

## Modify with Caution (Maintain Upstream Compatibility)

These files can be modified but must maintain upstream merge compatibility:

### Store Implementation
- `y-sweet-core/src/store/mod.rs` - Keep Extensions section clearly marked with `=== Extensions (start/end) ===` comments
- NEVER remove the Extension markers

### Server Endpoints
- `y-sweet/src/server.rs` - Keep basic endpoints from upstream, ensure `ext_routes()` is merged in `routes()` method
- Main concern: preserve the merge of extension routes

### Entry Point
- `y-sweet/src/main.rs` - Upstream base structure, preserve `tracing_setup` import

## Safe to Modify (Custom Extensions)

These files are fully custom and safe to modify freely:

### Extension Files
- `y-sweet-core/src/api_types_ext.rs` - Custom API type definitions
- `y-sweet/src/server_ext.rs` - Custom endpoint handlers
- `y-sweet/src/tracing_setup.rs` - Datadog APM initialization
- `y-sweet-core/src/store/store_ext.rs` - Extension trait documentation

### Store Implementations
- `y-sweet-core/src/store/s3.rs` - Custom AWS SDK implementation
- `y-sweet/src/stores/filesystem.rs` - When implementing extension methods

## Extension Implementation Rules

### 1. API Types
- **DO**: Add new types to `api_types_ext.rs`
- **DON'T**: Add custom types to `api_types.rs`

### 2. Server Endpoints
- **DO**: Add handlers to `server_ext.rs`
- **DO**: Add routes to `ext_routes()` or `ext_single_doc_routes()`
- **DON'T**: Add custom endpoints directly to `server.rs`

### 3. Store Methods
- **DO**: Add extension methods to Store trait between `=== Extensions (start/end) ===` markers
- **DO**: Implement in both s3.rs and filesystem.rs
- **DON'T**: Remove the marker comments

### 4. Dependencies
Mark custom dependencies in Cargo.toml:
```toml
# Custom dependencies for extensions
ddtrace = { version = "0.2.1", features = ["axum"] }  # Datadog APM integration
```

## Quick Reference: Adding a New Feature

### New Custom Endpoint
1. Define request/response types in `api_types_ext.rs`
2. Implement handler in `server_ext.rs`
3. Add route to `ext_routes()` or `ext_single_doc_routes()`
4. Use `/d/:doc_id` prefix (NOT `/doc/:doc_id`)
5. Run: `cargo build --all && cargo test --all && cargo fmt`

### New Store Extension Method
1. Add method signature to Store trait in `store/mod.rs` (between Extension markers)
2. Implement in `store/s3.rs`
3. Implement in `stores/filesystem.rs`
4. Run: `cargo build --all && cargo test --all && cargo fmt`

## Common Pitfalls to Avoid

❌ **DON'T:**
- Add custom endpoints directly to `server.rs`
- Add custom types to `api_types.rs`
- Remove Extensions markers from `store/mod.rs`
- Use `/doc/:doc_id` for new endpoints
- Forget to run verification commands

✅ **DO:**
- Add custom endpoints to `server_ext.rs`
- Add custom types to `api_types_ext.rs`
- Preserve Extensions markers
- Use `/d/:doc_id` for new endpoints
- Always run build, test, and format checks
