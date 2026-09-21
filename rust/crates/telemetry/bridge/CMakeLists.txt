include(FetchContent)
FetchContent_Declare(
  Corrosion
  GIT_REPOSITORY https://github.com/corrosion-rs/corrosion.git
  GIT_TAG v0.6.1)
FetchContent_MakeAvailable(Corrosion)

if(EMSCRIPTEN AND BUILD_QUENT_BROWSER_TELEMETRY)
  corrosion_import_crate(
    MANIFEST_PATH Cargo.toml
    CRATES duckdb-telemetry-bridge
    FEATURES browser
    FLAGS -Zbuild-std=std,panic_abort)
  corrosion_set_env_vars(
    duckdb_telemetry_bridge
    "RUSTC_BOOTSTRAP=1")
else()
  corrosion_import_crate(
    MANIFEST_PATH Cargo.toml
    CRATES duckdb-telemetry-bridge)
endif()

if(EMSCRIPTEN AND BUILD_QUENT_BROWSER_TELEMETRY)
  target_compile_definitions(duckdb_telemetry_bridge INTERFACE DUCKDB_QUENT_BROWSER_TELEMETRY)
endif()

set(TELEMETRY_INCLUDE_PROFILE native)
if(EMSCRIPTEN AND BUILD_QUENT_BROWSER_TELEMETRY)
  set(TELEMETRY_INCLUDE_PROFILE browser)
endif()

set_target_properties(
  duckdb_telemetry_bridge
  PROPERTIES ARCHIVE_OUTPUT_DIRECTORY
             "${PROJECT_BINARY_DIR}/${INSTALL_LIB_DIR}")

# build.rs copies rust/cxx.h and all generated bridge headers here.
target_include_directories(
  duckdb_telemetry_bridge
  INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include/${TELEMETRY_INCLUDE_PROFILE}>
    $<INSTALL_INTERFACE:${INSTALL_INCLUDE_DIR}>)

if(DEFINED DUCKDB_EXPORT_SET)
  corrosion_install(
    TARGETS duckdb_telemetry_bridge
    EXPORT "${DUCKDB_EXPORT_SET}")
endif()
