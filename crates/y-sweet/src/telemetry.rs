use anyhow::Result;
use ddtrace::{
    formatter::DatadogFormatter,
    set_global_propagator,
    tracer::{self, ProviderGuard},
};
use std::env;
use tracing_subscriber::EnvFilter;
use url::Url;

#[derive(Default)]
struct TraceBootstrapDiagnostics {
    configured_endpoint: Option<String>,
    endpoint_source: Option<&'static str>,
    headers_source: Option<&'static str>,
    requires_api_key: bool,
    missing_api_key: bool,
}

struct EndpointSetting {
    value: String,
    source: &'static str,
    requires_api_key: bool,
}

fn non_empty_env(key: &str) -> Option<String> {
    match env::var(key) {
        Ok(value) if !value.trim().is_empty() => Some(value),
        _ => None,
    }
}

fn extract_port(endpoint: &str) -> Option<u16> {
    if let Ok(url) = Url::parse(endpoint) {
        return url.port();
    }
    if let Ok(url) = Url::parse(&format!("http://{endpoint}")) {
        return url.port();
    }
    None
}

fn sanitize_endpoint(endpoint: &str) -> String {
    if let Ok(url) = Url::parse(endpoint) {
        let host = url.host_str().unwrap_or(endpoint);
        return match url.port() {
            Some(port) => format!("{}://{}:{}", url.scheme(), host, port),
            None => format!("{}://{}", url.scheme(), host),
        };
    }
    if let Ok(url) = Url::parse(&format!("http://{endpoint}")) {
        let host = url.host_str().unwrap_or(endpoint);
        return match url.port() {
            Some(port) => format!("{}://{}:{}", url.scheme(), host, port),
            None => format!("http://{}", host),
        };
    }
    endpoint.to_string()
}

fn endpoint_requires_datadog_api_key(endpoint: &str) -> bool {
    Url::parse(endpoint)
        .or_else(|_| Url::parse(&format!("http://{endpoint}")))
        .ok()
        .and_then(|url| url.host_str().map(|host| host.contains("datadoghq")))
        .unwrap_or(false)
}

fn user_defined_endpoint() -> Option<EndpointSetting> {
    if let Some(value) = non_empty_env("Y_SWEET_OTLP_ENDPOINT") {
        return Some(EndpointSetting {
            value,
            source: "Y_SWEET_OTLP_ENDPOINT",
            requires_api_key: false,
        });
    }

    if let Some(value) = non_empty_env("DD_OTLP_GRPC_ENDPOINT") {
        return Some(EndpointSetting {
            requires_api_key: endpoint_requires_datadog_api_key(&value),
            value,
            source: "DD_OTLP_GRPC_ENDPOINT",
        });
    }

    if let Some(value) = non_empty_env("DD_TRACE_AGENT_URL") {
        if matches!(extract_port(&value), Some(4317 | 4318)) {
            return Some(EndpointSetting {
                requires_api_key: endpoint_requires_datadog_api_key(&value),
                value,
                source: "DD_TRACE_AGENT_URL",
            });
        }
    }

    None
}

fn agent_endpoint_from_env() -> Option<EndpointSetting> {
    let host = non_empty_env("Y_SWEET_DATADOG_AGENT_HOST")
        .or_else(|| non_empty_env("DD_OTLP_GRPC_HOST"))
        .or_else(|| non_empty_env("DD_AGENT_HOST"))
        .or_else(|| non_empty_env("DD_TRACE_AGENT_HOSTNAME"))?;

    let port = non_empty_env("Y_SWEET_DATADOG_AGENT_PORT")
        .or_else(|| non_empty_env("DD_OTLP_GRPC_PORT"))
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(4317);

    let scheme = non_empty_env("Y_SWEET_DATADOG_AGENT_SCHEME")
        .or_else(|| non_empty_env("DD_OTLP_GRPC_SCHEME"))
        .unwrap_or_else(|| "http".to_string());

    Some(EndpointSetting {
        value: format!("{}://{}:{}", scheme, host, port),
        source: "DD_AGENT_HOST",
        requires_api_key: false,
    })
}

fn direct_ingest_endpoint() -> Option<EndpointSetting> {
    if non_empty_env("DD_AGENT_HOST").is_some()
        || non_empty_env("DD_TRACE_AGENT_HOSTNAME").is_some()
        || non_empty_env("Y_SWEET_DATADOG_AGENT_HOST").is_some()
    {
        return None;
    }

    let _api_key = non_empty_env("DD_API_KEY")?;
    let site = non_empty_env("DD_SITE").unwrap_or_else(|| "datadoghq.com".to_string());

    Some(EndpointSetting {
        value: format!("https://otlp.{}:4317", site),
        source: "DD_SITE",
        requires_api_key: true,
    })
}

fn configure_datadog_otlp_env() -> TraceBootstrapDiagnostics {
    const OTEL_TRACES_ENDPOINT: &str = "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT";
    const OTEL_GENERAL_ENDPOINT: &str = "OTEL_EXPORTER_OTLP_ENDPOINT";
    const OTEL_TRACES_HEADERS: &str = "OTEL_EXPORTER_OTLP_TRACES_HEADERS";
    const OTEL_GENERAL_HEADERS: &str = "OTEL_EXPORTER_OTLP_HEADERS";

    let mut diagnostics = TraceBootstrapDiagnostics::default();

    let preconfigured_endpoint =
        non_empty_env(OTEL_TRACES_ENDPOINT).or_else(|| non_empty_env(OTEL_GENERAL_ENDPOINT));

    let endpoint_setting = if let Some(value) = preconfigured_endpoint.clone() {
        Some(EndpointSetting {
            requires_api_key: endpoint_requires_datadog_api_key(&value),
            source: "preconfigured",
            value,
        })
    } else {
        user_defined_endpoint()
            .or_else(agent_endpoint_from_env)
            .or_else(direct_ingest_endpoint)
    };

    if let Some(endpoint) = endpoint_setting {
        if preconfigured_endpoint.is_none() {
            env::set_var(OTEL_TRACES_ENDPOINT, &endpoint.value);
        }
        diagnostics.requires_api_key = endpoint.requires_api_key;
        diagnostics.endpoint_source = Some(endpoint.source);
        diagnostics.configured_endpoint = Some(endpoint.value);
    }

    let headers_already_set = non_empty_env(OTEL_TRACES_HEADERS).is_some()
        || non_empty_env(OTEL_GENERAL_HEADERS).is_some();

    if diagnostics.requires_api_key && !headers_already_set {
        match non_empty_env("DD_API_KEY") {
            Some(api_key) => {
                env::set_var(OTEL_GENERAL_HEADERS, format!("DD-API-KEY={}", api_key));
                diagnostics.headers_source = Some("DD_API_KEY");
            }
            None => {
                diagnostics.missing_api_key = true;
            }
        }
    }

    diagnostics
}

pub fn init_tracing(filter: EnvFilter, app_version: &str) -> Result<Option<ProviderGuard>> {
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    let tracing_disabled = env::var("DD_TRACE_ENABLED")
        .map(|value| matches!(value.as_str(), "0") || value.eq_ignore_ascii_case("false"))
        .unwrap_or(false);

    if tracing_disabled {
        let fmt_layer = tracing_subscriber::fmt::layer()
            .json()
            .with_timer(tracing_subscriber::fmt::time::UtcTime::rfc_3339())
            .with_target(false)
            .with_thread_ids(false)
            .with_thread_names(false)
            .with_file(false)
            .with_line_number(false)
            .with_current_span(true)
            .with_span_list(false)
            .with_span_events(tracing_subscriber::fmt::format::FmtSpan::NONE);

        tracing_subscriber::registry()
            .with(filter)
            .with(fmt_layer)
            .init();

        return Ok(None);
    }

    let bootstrap_diagnostics = configure_datadog_otlp_env();

    set_global_propagator();

    if env::var("DD_VERSION").is_err() {
        env::set_var("DD_VERSION", app_version);
    }

    let service_name = env::var("DD_SERVICE").unwrap_or_else(|_| "y-sweet".to_string());

    match tracer::build_layer(service_name) {
        Ok((datadog_layer, guard)) => {
            let fmt_layer = tracing_subscriber::fmt::layer()
                .json()
                .with_timer(tracing_subscriber::fmt::time::UtcTime::rfc_3339())
                .with_target(false)
                .with_thread_ids(false)
                .with_thread_names(false)
                .with_file(false)
                .with_line_number(false)
                .with_current_span(true)
                .with_span_list(false)
                .with_span_events(tracing_subscriber::fmt::format::FmtSpan::NONE)
                .event_format(DatadogFormatter);

            tracing_subscriber::registry()
                .with(filter)
                .with(fmt_layer)
                .with(datadog_layer)
                .init();

            tracing::debug!(
                message = "Datadog tracing exporter configured",
                event = "datadog_tracing_configured",
                endpoint = bootstrap_diagnostics
                    .configured_endpoint
                    .as_deref()
                    .map(|value| sanitize_endpoint(value)),
                source = bootstrap_diagnostics.endpoint_source.unwrap_or("unknown")
            );

            if let Some(source) = bootstrap_diagnostics.headers_source {
                tracing::debug!(
                    message = "Datadog OTLP authentication headers configured",
                    event = "datadog_headers_configured",
                    source = %source
                );
            }

            if bootstrap_diagnostics.missing_api_key {
                tracing::warn!(
                    message = "DD_API_KEY is not set but is required for Datadog OTLP ingestion",
                    event = "datadog_missing_api_key"
                );
            }

            Ok(Some(guard))
        }
        Err(err) => {
            let fmt_layer = tracing_subscriber::fmt::layer()
                .json()
                .with_timer(tracing_subscriber::fmt::time::UtcTime::rfc_3339())
                .with_target(false)
                .with_thread_ids(false)
                .with_thread_names(false)
                .with_file(false)
                .with_line_number(false)
                .with_current_span(true)
                .with_span_list(false)
                .with_span_events(tracing_subscriber::fmt::format::FmtSpan::NONE);

            tracing_subscriber::registry()
                .with(filter)
                .with(fmt_layer)
                .init();

            eprintln!("datadog tracer initialization failed, continuing without APM: {err}");

            Ok(None)
        }
    }
}
