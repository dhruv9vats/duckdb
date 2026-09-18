use quent_codegen::CxxOptions;

fn main() {
    let mut builder = duckdb_telemetry_model::DuckDBModel::build("DuckDB");
    builder.nvtx = false;
    let options = CxxOptions {
        crate_name: "duckdb-telemetry-bridge".into(),
        instrumentation_crate: "duckdb_telemetry_model".into(),
        ..Default::default()
    };

    let files = quent_codegen::emit_cxx(&builder, &options);
    let bridge_files = quent_codegen::write_bridge_files(&files, &options);
    cxx_build::bridges(bridge_files)
        .std("c++20")
        .compile("duckdb_telemetry_bridge");
    quent_codegen::copy_cxx_headers();
}
