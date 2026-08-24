include(FetchContent)
FetchContent_Declare(
  Corrosion
  GIT_REPOSITORY https://github.com/corrosion-rs/corrosion.git
  GIT_TAG v0.6.1)
FetchContent_MakeAvailable(Corrosion)

corrosion_import_crate(
  MANIFEST_PATH Cargo.toml
  CRATES duckdb-telemetry-bridge)

set_target_properties(
  duckdb_telemetry_bridge
  PROPERTIES ARCHIVE_OUTPUT_DIRECTORY
             "${PROJECT_BINARY_DIR}/${INSTALL_LIB_DIR}")

# build.rs copies rust/cxx.h and all generated bridge headers here.
target_include_directories(
  duckdb_telemetry_bridge
  INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${INSTALL_INCLUDE_DIR}>)

if(DEFINED DUCKDB_EXPORT_SET)
  corrosion_install(
    TARGETS duckdb_telemetry_bridge
    EXPORT "${DUCKDB_EXPORT_SET}")
endif()
