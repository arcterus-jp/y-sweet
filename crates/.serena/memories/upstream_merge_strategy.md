# Upstream Merge Strategy

## Overview
This fork maintains compatibility with jamsocket/y-sweet while adding custom extensions. Regular upstream merges are essential for security updates, bug fixes, and new features.

## Pre-Merge Preparation

### 1. Add upstream remote (first time only)
```bash
git remote add upstream https://github.com/jamsocket/y-sweet.git
git fetch upstream --tags
```

### 2. Tag current state
```bash
git tag -a pre-upstream-merge-$(date +%Y%m%d) -m "Before upstream merge"
```

### 3. Create sync branch
```bash
git checkout -b upstream-sync main
```

## Merge Execution

```bash
git fetch upstream
git merge upstream/main --no-ff
```

## Conflict Resolution Guide

| File | Resolution Strategy | Notes |
|------|---------------------|-------|
| `store/s3.rs` | **Keep ours** | We use aws-sdk-s3, upstream uses rusty-s3 |
| `store/mod.rs` | **Manual** | Preserve `=== Extensions (start/end) ===` markers |
| `api_types.rs` | **Take theirs** | Extensions are in `api_types_ext.rs` |
| `server.rs` | **Manual** | Keep base routes from upstream, preserve `ext_routes()` merge |
| `server_ext.rs` | **Keep ours** | Fully custom extension file |
| `api_types_ext.rs` | **Keep ours** | Fully custom extension file |
| `tracing_setup.rs` | **Keep ours** | Custom Datadog APM integration |
| `Cargo.toml` | **Manual** | Merge dependencies, keep custom ones with comments |
| `main.rs` | **Manual** | Keep upstream base, preserve `tracing_setup` import |

## Post-Merge Verification

### 1. Run mandatory checks
```bash
cargo build --all
cargo test --all
cargo fmt -- --check
```

### 2. Manual API verification
```bash
# Start server
make http.dev

# Test basic upstream APIs
curl http://localhost:8080/doc/new -X POST
curl http://localhost:8080/doc/{doc_id}/auth -X POST

# Test custom extension APIs
curl http://localhost:8080/d/{doc_id}/assets -X POST -d '{"contentType":"text/plain"}'
curl http://localhost:8080/d/{doc_id}/copy -X POST -d '{"destinationDocId":"new-doc"}'
curl http://localhost:8080/d/{doc_id} -X DELETE
```

### 3. Commit and merge
```bash
git add .
git commit -m "Merge upstream vX.Y.Z

- Integrate [upstream features]
- Preserve extension functionality
- All tests passing"

git checkout main
git merge upstream-sync --no-ff
```

## Merge Frequency Recommendations

| Scenario | Frequency | Priority |
|----------|-----------|----------|
| Security fixes | Immediate | Critical |
| Bug fixes | 1-2 weeks | High |
| New features | Monthly | Medium |
| Breaking changes | Careful evaluation | Assess impact |

## Common Merge Issues

### Server::new() signature changes
Upstream may add new parameters. Update all call sites:
```rust
// Example: new parameters added
Server::new(
    store,
    checkpoint_freq,
    authenticator,
    url_prefix,
    cancellation_token,
    doc_gc,
    max_body_size,  // New parameter
    skip_gc,        // New parameter
)
```

### Store trait conflicts
Always preserve the Extensions section markers:
```rust
// === Extensions (start) ===
async fn generate_upload_presigned_url(...);
// === Extensions (end) ===
```

### Extension routes not working
Verify `server.rs` routes() method merges extension routes:
```rust
pub fn routes(self: &Arc<Self>) -> Router {
    let base_routes = Router::new()
        // ... base routes ...
        .with_state(self.clone());
    
    // MUST merge extension routes
    base_routes.merge(crate::server_ext::ext_routes(self))
}
```
