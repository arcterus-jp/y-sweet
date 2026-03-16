use anyhow::Result;
use ddtrace::{
    formatter::DatadogFormatter,
    set_global_propagator,
    tracer::{self, ProviderGuard},
};
use tracing_subscriber::EnvFilter;

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Initializes tracing with optional Datadog APM integration
///
/// This function sets up tracing with JSON-formatted logs and optionally
/// integrates with Datadog APM if DD_TRACE_ENABLED is not set to false.
///
/// # Arguments
///
/// * `filter` - The EnvFilter to apply to the tracing subscriber
///
/// # Returns
///
/// Returns an optional ProviderGuard that must be kept alive for the duration
/// of the program to maintain the Datadog tracer connection.
pub fn init_tracing(filter: EnvFilter) -> Result<Option<ProviderGuard>> {
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

    let tracing_disabled = std::env::var("DD_TRACE_ENABLED")
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

    set_global_propagator();

    if std::env::var("DD_VERSION").is_err() {
        std::env::set_var("DD_VERSION", VERSION);
    }

    let service_name = std::env::var("DD_SERVICE").unwrap_or_else(|_| "y-sweet".to_string());

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
