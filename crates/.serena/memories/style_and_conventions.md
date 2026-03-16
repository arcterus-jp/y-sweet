# Coding Style & Naming Conventions

## General Style
- **Edition**: Rust 2021
- **Indentation**: 4 spaces (Rust default)
- **Line Length**: Follow rustfmt defaults
- **Formatting Tool**: `cargo fmt` (rustfmt) - run before committing
- **Linting**: `cargo clippy --all-targets --all-features` - catch lint issues

## Naming Conventions
- **Modules & Functions**: `snake_case`
- **Types (Structs, Enums, Traits)**: `PascalCase`
- **Constants**: `SCREAMING_SNAKE_CASE`
- **Examples from codebase**:
  - Constants: `DEFAULT_S3_REGION`, `VERSION`, `S3_ACCESS_KEY_ID`
  - Structs: `Opts`, `ServSubcommand`
  - Functions: `parse_s3_config_from_env_and_args`, `get_store_from_opts`, `init_tracing`

## Documentation
- Use `///` comments for public API documentation, especially for cross-crate consumption
- Document behavior changes in commit messages and PR descriptions
- Keep module boundaries clear (e.g., authentication in auth.rs)

## Code Organization
- Mirror existing module boundaries
- Keep public API surface minimal and well-documented
- Prefer colocating tests with code (tests.rs or tests modules)
- Use `#[tokio::test]` for async test scenarios

## Dependencies
- Favor async/await patterns with Tokio
- Use tracing for structured logging
- Leverage serde for serialization needs
- Follow established patterns for error handling (anyhow, thiserror)

## Extension Implementation Patterns

### File Naming for Extensions
- **Extension files**: Use `*_ext.rs` suffix (e.g., `server_ext.rs`, `api_types_ext.rs`)
- **Extension functions**: Use `ext_*` prefix (e.g., `ext_routes()`, `ext_single_doc_routes()`)
- **Avoid organization-specific names**: Use generic terms like "Extensions" or "Custom" instead

### API Endpoint Conventions
- **New custom endpoints**: Use `/d/:doc_id` prefix
- **Legacy endpoints**: `/doc/:doc_id` (deprecated, managed by upstream)
- **Example custom endpoints**:
  - `DELETE /d/:doc_id` - Document deletion
  - `POST /d/:doc_id/copy` - Document copy
  - `POST /d/:doc_id/assets` - Presigned URL generation
  - `GET /d/:doc_id/assets` - Asset listing

### Store Trait Extensions
When adding extension methods to the Store trait in `store/mod.rs`, use clear marker comments:

```rust
pub trait Store: Send + Sync {
    // Basic methods (upstream compatible)
    async fn init(&self) -> Result<()>;
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>>;
    // ... other basic methods ...

    // === Extensions (start) ===
    // Custom extension methods for presigned URLs and advanced operations
    async fn generate_upload_presigned_url(&self, key: &str, content_type: &str) -> Result<String>;
    async fn generate_download_presigned_url(&self, key: &str) -> Result<String>;
    // === Extensions (end) ===
}
```

**Never remove the Extensions markers** - they're critical for upstream merge conflict resolution.
