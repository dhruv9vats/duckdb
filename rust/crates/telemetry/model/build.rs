// Capture this crate's git provenance for the model.qmi sidecar.
use std::path::Path;

use quent_instrumentation_build::{Options, generate};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    quent_build_info::emit_source();

    let model = Path::new(env!("CARGO_MANIFEST_DIR")).join("model.yaml");
    println!("cargo:rerun-if-changed={}", model.display());

    let parsed = quent_yaml::parse_from_file(&model)?;
    for warning in &parsed.warnings {
        println!("cargo:warning={warning}");
    }

    generate(
        &parsed.schema,
        &Options {
            analyzer_package: Some("duckdb-telemetry-analyzer".to_owned()),
            collector_sink: std::env::var_os("CARGO_FEATURE_BROWSER").is_none(),
            serde: true,
            umbrella_event: true,
            ..Options::default()
        },
    )?;

    Ok(())
}
