# Claude Code Rules for Y-Sweet Project

## Project Context

This is a fork of jamsocket/y-sweet with custom extensions. The project maintains compatibility with upstream while adding custom functionality for presigned URLs, document management, and Datadog APM integration.

## Critical Rules

### 1. File Modification Restrictions

**DO NOT modify these files without explicit user permission:**
- `y-sweet-core/src/auth.rs` - Upstream compatible
- `y-sweet-core/src/doc_connection.rs` - Upstream compatible
- `y-sweet-core/src/doc_sync.rs` - Upstream compatible
- `y-sweet-core/src/sync/` directory - Upstream compatible
- `y-sweet/src/cli.rs` - Upstream compatible

**Modify with caution (maintain upstream compatibility):**
- `y-sweet-core/src/store/mod.rs` - Keep Extensions section marked
- `y-sweet/src/server.rs` - Keep basic endpoints only, merge ext_routes()
- `y-sweet/src/main.rs` - Entry point

**Safe to modify (custom extensions):**
- `y-sweet-core/src/api_types_ext.rs` - Custom API types
- `y-sweet/src/server_ext.rs` - Custom endpoints
- `y-sweet-core/src/store/s3.rs` - Custom S3 implementation

### 2. Extension Implementation Pattern

When adding new custom functionality:

1. **API Types:** Add to `api_types_ext.rs`, never to `api_types.rs`
2. **Endpoints:** Add to `server_ext.rs`, never directly to `server.rs`
3. **Store Methods:** Add to Extensions section in `store/mod.rs` with clear markers

### 3. Mandatory Verification

After ANY code change, you MUST run:

```bash
cargo build --all
cargo test --all
cargo fmt -- --check
```

All three MUST pass before considering the work complete.

### 4. Naming Conventions

- Extension files: Use `*_ext.rs` suffix
- Extension functions: Use `ext_*` prefix
- Route functions: `ext_routes()`, `ext_single_doc_routes()`
- Avoid organization-specific names (use "Extensions", "Custom" instead)

### 5. Endpoint Path Convention

- New endpoints: Use `/d/:doc_id` prefix
- Legacy endpoints: `/doc/:doc_id` (deprecated, managed by upstream)

### 6. Store Trait Extensions

When modifying `store/mod.rs`:

```rust
pub trait Store: Send + Sync {
    // Basic methods (upstream compatible)
    async fn init(&self) -> Result<()>;
    // ... other basic methods ...

    // === Extensions (start) ===
    // Custom extension methods here
    // === Extensions (end) ===
}
```

**Never remove the Extensions markers.**

### 7. Dependencies

- Use `aws-sdk-s3` (not rusty-s3 like upstream)
- Mark custom dependencies with comments in Cargo.toml
- Example: `ddtrace = "0.2.1"  # Custom: Datadog APM integration`

### 8. Testing Requirements

- All new endpoints must have tests in `server.rs` test module or `tests.rs`
- Import from `server_ext` module: `use crate::server_ext::{function_name};`

### 9. Conflict Resolution Strategy

When merging from upstream:

| File | Strategy |
|------|----------|
| `store/s3.rs` | Keep ours (custom AWS SDK) |
| `store/mod.rs` | Manual (preserve Extensions) |
| `api_types.rs` | Take theirs (extensions are separate) |
| `server.rs` | Manual (preserve ext_routes() merge) |
| `server_ext.rs` | Keep ours (fully custom) |

### 10. Common Pitfalls to Avoid

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

## Quick Reference

### Adding a New Custom Endpoint

1. Define types in `api_types_ext.rs`
2. Implement handler in `server_ext.rs`
3. Add route to `ext_routes()` or `ext_single_doc_routes()`
4. Run verification: `cargo build --all && cargo test --all && cargo fmt`

### Adding a Store Extension Method

1. Add method to Extensions section in `store/mod.rs`
2. Implement in `store/s3.rs`
3. Implement in `stores/filesystem.rs`
4. Run verification

## Documentation

See `DEVELOPMENT_RULES.md` for detailed guidelines.
