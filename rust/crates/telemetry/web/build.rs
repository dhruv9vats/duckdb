use std::{env, fs, path::Path, process::Command};

use sha2::{Digest, Sha256};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let schema = Path::new(env!("CARGO_MANIFEST_DIR")).join("../model/model.yaml");
    println!("cargo:rerun-if-changed={}", schema.display());
    println!("cargo:rerun-if-env-changed=DUCKDB_TELEMETRY_BUILD_ID");

    let schema_hash = format!("{:x}", Sha256::digest(fs::read(schema)?));
    let build_id = match env::var("DUCKDB_TELEMETRY_BUILD_ID") {
        Ok(build_id) => build_id,
        Err(_) => source_id()?,
    };
    let generated = format!(
        "pub const SCHEMA_HASH: &str = {schema_hash:?};\npub const BUILD_ID: &str = {build_id:?};\n"
    );
    fs::write(
        Path::new(&env::var("OUT_DIR")?).join("manifest.rs"),
        generated,
    )?;

    Ok(())
}

fn source_id() -> Result<String, Box<dyn std::error::Error>> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../..");
    let revision = git(&root, &["rev-parse", "HEAD"])?;
    let status = git(
        &root,
        &["status", "--porcelain", "--untracked-files=normal"],
    )?;
    let dirty = if status.is_empty() { "" } else { ".dirty" };

    Ok(format!("{revision}{dirty}+browser-v1"))
}

fn git(root: &Path, args: &[&str]) -> Result<String, Box<dyn std::error::Error>> {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(args)
        .output()?;
    if !output.status.success() {
        return Err(format!("git {} failed", args.join(" ")).into());
    }

    Ok(String::from_utf8(output.stdout)?.trim().to_owned())
}
