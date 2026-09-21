use std::{net::ToSocketAddrs, path::PathBuf};

use clap::Parser;
use duckdb_telemetry_analyzer::{DuckDbUiAnalyzer, Viewer};
use duckdb_telemetry_model as instrumentation;
use duckdb_telemetry_store::DuckDb;
use quent_analyzer::context::index_contexts;
use quent_io::ExporterOptions;
use quent_io::filesystem::{self, Format};
use quent_query_engine_analyzer::ui::QuentViewer;
use quent_query_engine_server::{
    analyzer_service_router_with_routes, collector_service, initialize_tracing,
};
use quent_store::event::{ModelEventStore, filesystem::Store};
use tokio::net::TcpListener;

type DuckDbContext = instrumentation::Context<instrumentation::DuckDb>;

fn api_guard() -> axum::Router {
    axum::Router::new().route(
        "/api/{*path}",
        axum::routing::any(|| async { axum::http::StatusCode::NOT_FOUND }),
    )
}

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "info")]
    log_level: String,

    #[arg(
        long,
        default_value = "[::]:7836",
        env = "QUENT_COLLECTOR_BIND_ADDRESS"
    )]
    collector_address: String,

    #[arg(long, default_value = "ndjson", env = "QUENT_COLLECTOR_EXPORTER")]
    exporter: String,

    #[arg(long, default_value = "events", env = "QUENT_COLLECTOR_OUTPUT_DIR")]
    output_dir: PathBuf,

    #[arg(long, default_value = "[::]:8080", env = "QUENT_ANALYZER_ADDRESS")]
    analyzer_address: String,

    #[arg(long, env = "QUENT_ANALYZER_CORS_ADDRESS")]
    cors_address: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    initialize_tracing(&args.log_level);

    let collector_addr = args
        .collector_address
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| format!("unable to resolve {}", args.collector_address))?;
    let analyzer_addr = args
        .analyzer_address
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| format!("unable to resolve {}", args.analyzer_address))?;

    let format = match args.exporter.as_str() {
        "ndjson" => Format::Ndjson,
        "msgpack" => Format::Msgpack,
        "postcard" => Format::Postcard,
        other => return Err(format!("unknown exporter: {other}").into()),
    };
    let exporter = ExporterOptions::FileSystem(filesystem::exporter::Options::new(
        format,
        args.output_dir.clone(),
    ));

    let collector = async {
        collector_service::<DuckDbContext, _>(move |id| {
            DuckDbContext::try_with_id(id, exporter.clone()).map_err(|error| error.to_string())
        })?
        .serve(collector_addr)
        .await
        .map_err(|error| -> Box<dyn std::error::Error> { Box::new(error) })
    };

    let importer_dir = args.output_dir.clone();
    let index_dir = args.output_dir;
    let importer = move |context_id| {
        let events = Store::<DuckDb>::new(&importer_dir)
            .events(context_id)
            .map_err(quent_io::ImporterError::other)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(quent_io::ImporterError::other)?;
        Ok::<Box<dyn Iterator<Item = _>>, quent_query_engine_server::error::ServerError>(Box::new(
            events.into_iter(),
        ))
    };
    let lister = move || {
        index_contexts(&index_dir, |context_dir| {
            Ok(Viewer::context_inventory(context_dir)?)
        })
    };
    let analyzer = async {
        axum::serve(
            TcpListener::bind(analyzer_addr).await?,
            analyzer_service_router_with_routes::<DuckDbUiAnalyzer>(
                Box::new(importer),
                Box::new(lister),
                args.cors_address,
                api_guard(),
            )?
            .into_make_service(),
        )
        .await?;
        Ok::<(), Box<dyn std::error::Error>>(())
    };

    tracing::info!("listening on {collector_addr} and {analyzer_addr}");
    tokio::try_join!(collector, analyzer)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use axum::{
        Router,
        body::Body,
        http::{Request, StatusCode},
        routing::get,
    };
    use tower::ServiceExt;

    use super::api_guard;

    #[tokio::test]
    async fn api_guard_precedes_spa() {
        let router = Router::new()
            .route("/api/engines", get(|| async { StatusCode::OK }))
            .merge(api_guard())
            .fallback(get(|| async { StatusCode::OK }));

        for (path, expected) in [
            ("/api/engines", StatusCode::OK),
            ("/api/nvtx/context", StatusCode::NOT_FOUND),
            ("/query", StatusCode::OK),
        ] {
            let request = Request::builder().uri(path).body(Body::empty()).unwrap();
            let response = router.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), expected, "{path}");
        }
    }
}
