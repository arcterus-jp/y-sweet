# Testing Guidelines

## Test Philosophy
- Prefer small, deterministic tests
- Colocate tests with code (tests.rs or tests modules within each crate)
- Assert on both content and side effects
- Pay special attention to S3 stores and document sync flows

## Test Structure
Tests are located in:
- `y-sweet/src/tests.rs` - Main integration tests
- `y-sweet/src/server.rs` - Server-specific tests (line 1766+)
- Individual crate test modules - Unit tests

## Async Testing
Use `#[tokio::test]` for async scenarios:
```rust
#[tokio::test]
async fn test_async_operation() {
    // Test async code
}
```

## Test Fixtures
- Runtime artifacts and test data should be placed under `data/`
- LocalStack configuration in `localstack/` for S3 testing
- Use docker-compose for integration testing with S3

## Running Tests
```bash
cargo test                    # All tests
cargo test -p y-sweet-core    # Specific crate tests
cargo test --all-features     # With all features (CI requirement)
cargo test -- --nocapture     # Show test output
```

## Test Development Dependencies
Available in `[dev-dependencies]`:
- `tokio` with test features
- `http` for HTTP testing
- `dashmap` for concurrent data structures

## Best Practices
1. **Deterministic**: Tests should produce consistent results
2. **Isolated**: Each test should be independent
3. **Fast**: Keep tests quick to encourage frequent runs
4. **Clear Assertions**: Test both success and failure cases
5. **Coverage**: Focus on critical paths:
   - Document synchronization
   - Store operations (especially S3)
   - Authentication and authorization
   - Error handling

## Adding New Tests
When adding new functionality:
1. Write tests first or alongside implementation
2. Extend fixture data under `data/` if needed
3. Ensure `cargo test --all-features` continues to pass
4. Document any special test setup requirements
5. Consider both unit tests and integration scenarios

## S3 Testing
For S3-related features:
- Use LocalStack for local testing
- Test both path-style and virtual-hosted-style URLs
- Verify presigned URL generation and validation
- Test error handling for network failures
