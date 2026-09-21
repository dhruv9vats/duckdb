use std::path::Path;

use quent_store_build::{Options, generate};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let model = Path::new(env!("CARGO_MANIFEST_DIR")).join("../model/model.yaml");
    println!("cargo:rerun-if-changed={}", model.display());

    let parsed = quent_yaml::parse_from_file(&model)?;
    for warning in &parsed.warnings {
        println!("cargo:warning={warning}");
    }

    let generated = generate(
        &parsed.schema,
        &Options {
            umbrella_event: true,
            ..Options::default()
        },
    )?;
    if std::env::var_os("CARGO_FEATURE_NATIVE_IO").is_none() {
        let source = std::fs::read_to_string(&generated.path)?;
        let portable = source.replacen(
            "impl ::quent_store::event::filesystem::Model for DuckDb",
            "#[cfg(feature = \"native-io\")]\nimpl ::quent_store::event::filesystem::Model for DuckDb",
            1,
        );
        if portable == source {
            return Err("generated DuckDb filesystem model was not found".into());
        }
        std::fs::write(&generated.path, portable)?;
    }
    for warning in generated.warnings {
        println!("cargo:warning={warning}");
    }

    Ok(())
}
