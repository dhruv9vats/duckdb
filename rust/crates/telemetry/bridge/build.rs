use std::path::{Path, PathBuf};

use quent_schema_codegen_cpp::{Exporters, Options};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let model = Path::new(env!("CARGO_MANIFEST_DIR")).join("../model/model.yaml");
    println!("cargo:rerun-if-changed={}", model.display());

    let schema = quent_yaml::parse_from_file(&model)?.schema;
    let options = Options {
        crate_name: "duckdb-telemetry-bridge".to_owned(),
        instrumentation_path: "duckdb_telemetry_model".to_owned(),
        exporters: Exporters::all(),
        ..Options::default()
    };
    let files = quent_schema_codegen_cpp::emit(&schema, &options)?;
    let bridges = quent_schema_codegen_cpp::write_bridge_files(&files, &options)?;
    let mut build = cxx_build::bridges(bridges);
    let include_dir = quent_schema_codegen_cpp::stage_cxx_headers(&options)?;
    build.include(&include_dir).std("c++20");
    let staged_include = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("include");
    if staged_include.exists() {
        std::fs::remove_dir_all(&staged_include)?;
    }
    copy_headers(&include_dir, &staged_include)?;
    build.compile("duckdb_telemetry_bridge");

    Ok(())
}

fn copy_headers(source: &Path, target: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(target)?;
    for entry in std::fs::read_dir(source)? {
        let entry = entry?;
        let source_path = entry.path();
        let target_path = target.join(entry.file_name());
        if source_path.is_dir() {
            copy_headers(&source_path, &target_path)?;
        } else {
            std::fs::copy(source_path, target_path)?;
        }
    }

    Ok(())
}
