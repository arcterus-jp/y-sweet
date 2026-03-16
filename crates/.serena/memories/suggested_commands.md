# Suggested Commands for Y-Sweet Development

## Build & Compile
```bash
cargo build                  # Build the entire workspace
make build                   # Alternative: uses cargo build
cargo build --release        # Production build with optimizations
cargo build -p y-sweet-core  # Build specific crate
```

## Testing
```bash
cargo test                           # Run all tests
cargo test -p y-sweet-core           # Test specific crate
cargo test --all-features            # Run tests with all features enabled
cargo test -- --nocapture            # Show test output
```

## Code Quality
```bash
cargo fmt                            # Format all code (run before commits)
make format                          # Alternative: uses cargo fmt
cargo clippy --all-targets --all-features  # Lint checks
cargo check                          # Fast type checking without building
```

## Development Server
```bash
make http.dev                        # Launch dev server with auto-reload (cargo watch)
                                     # Uses local filesystem storage with ./data
                                     # Runs on http://localhost:8080

make http.dev.s3                     # Launch dev server with S3 storage
                                     # Points to LocalStack S3 emulator
```

## Docker & LocalStack
```bash
docker-compose up --build            # Start full stack with LocalStack S3
docker-compose down                  # Stop containers
docker-compose logs -f y-sweet       # View service logs
```

## Direct Cargo Commands
```bash
cargo run -- serve ./data --host 0.0.0.0 --url-prefix http://localhost:8080 --auth <KEY>
cargo run -- serve s3://bucket-name --host 0.0.0.0 --url-prefix <URL> --auth <KEY>
```

## Environment Setup
For local S3 development (outside Docker):
```bash
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_REGION=ap-northeast-1
export AWS_ENDPOINT_URL_S3=http://localhost:4566
export AWS_S3_USE_PATH_STYLE=true
```

## Utility Commands (Darwin/macOS)
Standard Unix commands work on Darwin:
```bash
ls, cd, pwd                          # Navigation
grep, find                           # Search
cat, head, tail                      # File viewing
git status, git diff, git log        # Git operations
```

## Workspace Commands
```bash
cargo clean                          # Remove build artifacts
cargo tree                           # Show dependency tree
cargo update                         # Update dependencies
cargo --version                      # Check Rust/Cargo version
```
