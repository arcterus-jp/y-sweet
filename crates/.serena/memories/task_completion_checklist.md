# Task Completion Checklist

When completing a coding task in the Y-Sweet project, follow these steps:

## 1. Mandatory Verification (MUST RUN after ANY code change)
```bash
cargo build --all                    # Build all packages
cargo test --all                     # Run all tests
cargo fmt -- --check                 # Verify formatting (or run `cargo fmt` to fix)
```

**All three MUST pass** before considering work complete. This is non-negotiable.

## 2. Code Quality Checks
```bash
cargo fmt                            # Format code
cargo clippy --all-targets --all-features  # Run linter (optional but recommended)
```

## 2. Testing
```bash
cargo test                           # Run all tests
cargo test --all-features            # Test with all features enabled
cargo test -p <crate-name>           # Test specific crate if needed
```

## 3. Build Verification
```bash
cargo build                          # Verify workspace builds
cargo check                          # Quick type check
```

## 4. Commit Guidelines
- Follow conventional commits format: `feat:`, `fix:`, `refactor:`, `test:`, `docs:`, etc.
- Use meaningful scopes: `feat(sync): add new sync mechanism`, `fix(s3): resolve presigned URL caching`
- Reference issues or PR numbers when available
- Keep commits atomic and focused

Example commit messages:
```
feat(store): add S3 presigned URL caching
fix(server): handle connection timeout gracefully
refactor(core): extract common sync logic
test(store): add integration tests for S3 adapter
```

## 5. Pull Request Preparation
- Write clear PR summary describing behavior changes
- List manual verification steps:
  - CLI commands tested
  - docker-compose scenarios verified
  - Test results included
- Attach screenshots or logs for user-facing changes
- Call out any migrations or configuration changes
- Ensure all CI checks pass (cargo test --all-features)

## 6. Documentation Updates
- Update `///` doc comments for public API changes
- Update README files if user-facing functionality changed
- Document new environment variables or configuration options

## 7. Local Testing Scenarios
Before pushing, verify:
- Local filesystem storage: `make http.dev`
- S3 storage with LocalStack: `make http.dev.s3` or `docker-compose up --build`
- Unit tests pass: `cargo test`
- Integration scenarios work as expected

## 8. Configuration & Environment
- Keep secrets out of the repository
- Use `.env` or shell exports for local development
- Document new environment variables in README or docker-compose.yml
- Verify Docker setup still works: `docker-compose up --build`
