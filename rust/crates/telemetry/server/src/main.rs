use std::{net::ToSocketAddrs, path::PathBuf};

use clap::Parser;
use duckdb_telemetry_analyzer::DuckDbUiAnalyzer;
use duckdb_telemetry_model::{DuckDB, DuckDBContext};
use quent_io::ExporterOptions;
use quent_io::filesystem::{self, Format};
use quent_query_engine_server::{
    analyzer_cache::index_query_engines, analyzer_service_router, collector_service,
    initialize_tracing,
};
use tokio::net::TcpListener;
use uuid::Uuid;

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
        collector_service::<DuckDBContext, _>(move |id| {
            DuckDBContext::try_with_id(id, Some(exporter.clone()))
                .map_err(|error| error.to_string())
        })?
        .serve(collector_addr)
        .await
        .map_err(|error| -> Box<dyn std::error::Error> { Box::new(error) })
    };

    let importer_dir = args.output_dir.clone();
    let index_dir = args.output_dir;
    let importer = move |context_id: Uuid| {
        Ok(DuckDB::import_events(
            &importer_dir.join(context_id.to_string()),
        )?)
    };
    let lister = move || index_query_engines(&index_dir);
    let analyzer = async {
        axum::serve(
            TcpListener::bind(analyzer_addr).await?,
            analyzer_service_router::<DuckDbUiAnalyzer>(
                Box::new(importer),
                Box::new(lister),
                args.cors_address,
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
