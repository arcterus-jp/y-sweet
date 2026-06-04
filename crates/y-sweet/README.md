<img src="https://raw.githubusercontent.com/drifting-in-space/y-sweet/main/logo.svg" />

# y-sweet: a Yjs server with persistence and auth

**y-sweet** is an open-source server for building realtime applications on top of the [Yjs](https://github.com/yjs/yjs) CRDT library.

## Features

- Persists document data to a network filesystem or S3-compatible storage, [inspired by Figma’s infrastructure](https://digest.browsertech.com/archive/browsertech-digest-figma-is-a-file-editor/).
- Scales horizontally with a [session backend](https://driftingin.space/posts/session-lived-application-backends) model.
- Deploys as a native Linux process, or as a WebAssembly module on Cloudflare's edge.
- Provides document-level access control via client tokens.
- Written in Rust with a focus on stability and performance, building on the excellent [y-crdt](https://github.com/y-crdt/y-crdt/) library.

## `y-sweet` crate

The y-sweet crate is primarily intended to be used as a binary, but can also be used as a library. See `main.rs` for usage examples.

## Tracing & Datadog Integration

y-sweet ships with an opinionated telemetry bootstrap (see `src/telemetry.rs`) that configures OpenTelemetry + Datadog automatically. By default the server emits JSON logs and exports trace data via OTLP/gRPC whenever Datadog tracing is enabled in the environment.

### Quick Start

1. **Sidecar/host agent (recommended)**
   ```bash
   export DD_AGENT_HOST=localhost
   export DD_OTLP_GRPC_PORT=4317        # optional, defaults to 4317
   export DD_SERVICE=y-sweet
   ./target/release/y-sweet serve --prod
   ```
   The bootstrap derives `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://$DD_AGENT_HOST:$PORT` and ships traces to your agent.

2. **Direct Datadog ingest (no agent)**
   ```bash
   export DD_API_KEY=xxxxx
   export DD_SITE=datadoghq.com         # optional
   ./target/release/y-sweet serve --prod
   ```
   When no agent host is present the exporter falls back to `https://otlp.$DD_SITE:4317` and adds the `DD-API-KEY` header automatically.

At runtime the telemetry bootstrap logs which endpoint and headers source were chosen (look for `event=datadog_tracing_configured` in stdout) so you can confirm the effective settings in production.

### Environment Variables

| Variable | Purpose | Default / Notes |
| --- | --- | --- |
| `DD_TRACE_ENABLED` | Disable all tracing when set to `0`/`false`. | Tracing is on unless explicitly disabled. |
| `DD_SERVICE` | Logical service name reported to Datadog. | `y-sweet`. |
| `DD_VERSION` | Service version. | Automatically set to `CARGO_PKG_VERSION` if unset. |
| `Y_SWEET_OTLP_ENDPOINT` | Hard override for the OTLP gRPC endpoint. | Highest precedence; skip the auto-detection logic. |
| `DD_OTLP_GRPC_ENDPOINT` | Datadog-specific gRPC endpoint override. | Used when present (and implies no header injection). |
| `DD_TRACE_AGENT_URL` | Fallback agent URL; respected only when the port is 4317/4318. | Useful when you already expose OTLP over HTTP(S). |
| `Y_SWEET_DATADOG_AGENT_HOST` / `DD_AGENT_HOST` / `DD_OTLP_GRPC_HOST` / `DD_TRACE_AGENT_HOSTNAME` | Hostname/IP for a Datadog agent accepting OTLP gRPC. | If any are set, the exporter targets `http://host:$PORT`. |
| `Y_SWEET_DATADOG_AGENT_PORT` / `DD_OTLP_GRPC_PORT` | Agent port. | Defaults to `4317`. |
| `Y_SWEET_DATADOG_AGENT_SCHEME` / `DD_OTLP_GRPC_SCHEME` | Override scheme when targeting HTTPS or custom proxies. | Defaults to `http`. |
| `DD_API_KEY` | Injected as `DD-API-KEY` header when sending directly to Datadog SaaS. | Required for direct ingest; ignored when talking to a local agent. |
| `DD_SITE` | Datadog site suffix used for SaaS ingest (`otlp.$DD_SITE`). | `datadoghq.com`. |
| `OTEL_EXPORTER_OTLP_*` | Standard OpenTelemetry overrides. | Automatically populated when unset; you can still set them yourself if you need full control. |

> Tip: Set `DD_TRACE_ENABLED=0` in local development if you want to keep the JSON log formatting but skip the Datadog exporter entirely.
