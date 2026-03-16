# Y-Sweet Project Overview

## Fork Information
This is a fork of [jamsocket/y-sweet](https://github.com/jamsocket/y-sweet) with custom extensions. The project maintains compatibility with upstream while adding:
- Presigned URL generation for S3 assets
- Document deletion and copying APIs
- Datadog APM integration
- Custom endpoint extensions under `/d/:doc_id` prefix

**Version**: Based on upstream v0.9.1 with custom extensions

## Purpose
Y-Sweet is an open-source server for building realtime applications on top of the Yjs CRDT library. It provides:
- Document persistence to network filesystem or S3-compatible storage (inspired by Figma's infrastructure)
- Horizontal scaling with a session backend model
- Native Linux process or WebAssembly deployment on Cloudflare's edge
- Document-level access control via client tokens
- Written in Rust with focus on stability and performance, building on y-crdt library

## Tech Stack
- **Language**: Rust 2021 edition
- **Async Runtime**: Tokio
- **Web Framework**: Axum
- **CRDT Implementation**: yrs, yrs-kvstore, lib0
- **Storage**: AWS S3 SDK (S3-compatible storage)
- **Observability**: tracing, tracing-subscriber, ddtrace (Datadog APM)
- **Serialization**: serde, serde_json, bincode
- **Build System**: Cargo (workspace-based)

## Workspace Structure
The project uses a Cargo workspace with the following members:
- **y-sweet**: Main service crate (CLI entrypoints, HTTP server)
- **y-sweet-core**: Reusable library (Sans-IO core with persistence, sync, and auth logic)
- **y-sweet-worker**: Excluded from workspace (separate CloudFlare Worker implementation)

### Key Directories
- `y-sweet/src/`: CLI entrypoints (main.rs, cli.rs, server.rs), storage adapters under stores/, tests in tests.rs
- `y-sweet-core/src/`: Shared logic in doc_sync/, store/, sync/
- `data/`: Sample fixtures for local development
- `localstack/`: S3 emulator setup for Docker development

## Environment & Configuration
The project runs on Darwin (macOS) with standard Rust tooling. Development can be done:
- Locally with filesystem storage
- With LocalStack S3 emulator via docker-compose
- Using environment variables for AWS credentials and endpoints
