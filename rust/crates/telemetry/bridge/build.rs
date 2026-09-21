use std::path::{Path, PathBuf};

use quent_schema_codegen_cpp::{Exporters, Options};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let model = Path::new(env!("CARGO_MANIFEST_DIR")).join("../model/model.yaml");
    println!("cargo:rerun-if-changed={}", model.display());

    let schema = quent_yaml::parse_from_file(&model)?.schema;
    let options = Options {
        crate_name: "duckdb-telemetry-bridge".to_owned(),
        instrumentation_path: "duckdb_telemetry_model".to_owned(),
        exporters: if std::env::var_os("CARGO_FEATURE_BROWSER").is_some() {
            Exporters::default()
        } else {
            Exporters::all()
        },
        ..Options::default()
    };
    let files = quent_schema_codegen_cpp::emit(&schema, &options)?;
    let bridges = quent_schema_codegen_cpp::write_bridge_files(&files, &options)?;
    let browser = std::env::var_os("CARGO_FEATURE_BROWSER").is_some();
    if browser {
        patch_browser_context(&bridges)?;
    }
    let mut build = cxx_build::bridges(bridges);
    let include_dir = quent_schema_codegen_cpp::stage_cxx_headers(&options)?;
    build.include(&include_dir).std("c++20");
    build.compile("duckdb_telemetry_bridge");

    let stage_name = if browser { "browser" } else { "native" };
    let staged_include = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("include")
        .join(stage_name);
    if staged_include.exists() {
        std::fs::remove_dir_all(&staged_include)?;
    }
    copy_headers(&include_dir, &staged_include)?;
    if browser {
        write_browser_facade(&staged_include)?;
    }

    Ok(())
}

fn write_browser_facade(include_dir: &Path) -> std::io::Result<()> {
    let header = include_dir.join("duckdb-telemetry-bridge/gen/quent.hpp");
    let mut source = std::fs::read_to_string(&header)?;
    let marker = "  static Context none();";
    if !source.contains(marker) {
        return Err(std::io::Error::other(
            "generated Context facade did not contain noop factory",
        ));
    }
    source = source.replacen(
        marker,
        "  static Context none();\n  static Context browser(std::uint64_t max_bytes);\n  BrowserBatch browser_drain(std::uint64_t max_bytes) const;\n  std::uint64_t browser_dropped() const;\n  std::vector<std::string> browser_query_ids() const;\n  std::string browser_context_id() const;\n  std::uint64_t browser_watermark() const;\n  bool browser_begin_run() const;",
        1,
    );
    let context_marker = "namespace quent {\nclass Context final {";
    let browser_batch = "namespace quent {\nstruct BrowserBatch final {\n  std::vector<std::uint8_t> payload;\n  std::uint32_t event_count;\n  std::uint64_t min_timestamp;\n  std::uint64_t max_timestamp;\n  bool failed;\n};\n\nclass Context final {";
    if !source.contains(context_marker) {
        return Err(std::io::Error::other(
            "generated Context facade did not contain Context class",
        ));
    }
    source = source.replacen(context_marker, browser_batch, 1);
    source = source.replacen(
        "#include \"duckdb-telemetry-bridge/gen/context.rs.h\"",
        "#include \"duckdb-telemetry-bridge/gen/context.rs.h\"",
        1,
    );
    source.push_str(
        "\nnamespace quent {\ninline Context Context::browser(std::uint64_t max_bytes) {\n  return Context(detail::create_browser_context(max_bytes));\n}\ninline BrowserBatch Context::browser_drain(std::uint64_t max_bytes) const {\n  auto batch = inner_->browser_drain(max_bytes);\n  return {{batch.payload.begin(), batch.payload.end()}, batch.event_count, batch.min_timestamp, batch.max_timestamp, batch.failed};\n}\ninline std::uint64_t Context::browser_dropped() const {\n  return inner_->browser_dropped();\n}\ninline std::vector<std::string> Context::browser_query_ids() const {\n  auto ids = inner_->browser_query_ids();\n  std::vector<std::string> result;\n  result.reserve(ids.size());\n  for (const auto &id : ids) {\n    result.emplace_back(static_cast<std::string>(id));\n  }\n  return result;\n}\ninline std::string Context::browser_context_id() const {\n  return static_cast<std::string>(inner_->browser_context_id());\n}\ninline std::uint64_t Context::browser_watermark() const {\n  return inner_->browser_watermark();\n}\ninline void Context::browser_begin_run() const {\n  inner_->browser_begin_run();\n}\n} // namespace quent\n",
    );
    source = source.replace(
        "inline void Context::browser_begin_run() const {\n  inner_->browser_begin_run();\n}",
        "inline bool Context::browser_begin_run() const {\n  return inner_->browser_begin_run();\n}",
    );
    std::fs::write(header, source)
}

fn patch_browser_context(bridges: &[PathBuf]) -> std::io::Result<()> {
    let context = bridges
        .iter()
        .find(|path| path.file_name().is_some_and(|name| name == "context.rs"))
        .ok_or_else(|| std::io::Error::other("generated Context bridge was not found"))?;
    let mut source = std::fs::read_to_string(context)?;
    let module_marker = "pub mod ffi {\n";
    let browser_batch = "pub mod ffi {\n    struct BrowserBatch {\n        payload: Vec<u8>,\n        event_count: u32,\n        min_timestamp: u64,\n        max_timestamp: u64,\n        failed: bool,\n    }\n\n";
    if !source.contains(module_marker) {
        return Err(std::io::Error::other(
            "generated Context bridge did not contain ffi module",
        ));
    }
    source = source.replacen(module_marker, browser_batch, 1);
    let marker = "        type Context;\n        fn create_context(options: Box<ExporterOptions>) -> Result<Box<Context>>;";
    let replacement = "        type Context;\n        fn create_context(options: Box<ExporterOptions>) -> Result<Box<Context>>;\n        fn create_browser_context(max_bytes: u64) -> Result<Box<Context>>;\n        fn browser_drain(self: &Context, max_bytes: u64) -> BrowserBatch;\n        fn browser_dropped(self: &Context) -> u64;\n        fn browser_query_ids(self: &Context) -> Vec<String>;\n        fn browser_context_id(self: &Context) -> String;\n        fn browser_watermark(self: &Context) -> u64;\n        fn browser_begin_run(self: &Context) -> bool;";
    if !source.contains(marker) {
        return Err(std::io::Error::other(
            "generated Context bridge did not match browser patch",
        ));
    }
    source = source.replacen(marker, replacement, 1);
    source.push_str("\n\npub use crate::browser::create_browser_context;\n");
    std::fs::write(context, source)
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
