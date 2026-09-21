cmake_minimum_required(VERSION 3.20)
project(duckdb_quent_browser LANGUAGES C CXX)

set(DUCKDB_BROWSER_STACK_SIZE 5242880)

if(NOT EMSCRIPTEN)
  message(FATAL_ERROR "Build with emcmake")
endif()

get_filename_component(DUCKDB_ROOT "${CMAKE_CURRENT_LIST_DIR}/../../.." ABSOLUTE)
file(SHA256 "${DUCKDB_ROOT}/rust/crates/telemetry/model/model.yaml" TELEMETRY_SCHEMA_HASH)
if(DEFINED ENV{DUCKDB_TELEMETRY_BUILD_ID})
  set(TELEMETRY_BUILD_ID "$ENV{DUCKDB_TELEMETRY_BUILD_ID}")
else()
  execute_process(
    COMMAND git -C "${DUCKDB_ROOT}" rev-parse HEAD
    OUTPUT_VARIABLE DUCKDB_GIT_HASH
    OUTPUT_STRIP_TRAILING_WHITESPACE
    COMMAND_ERROR_IS_FATAL ANY)
  execute_process(
    COMMAND git -C "${DUCKDB_ROOT}" status --porcelain --untracked-files=normal
    OUTPUT_VARIABLE DUCKDB_GIT_STATUS
    OUTPUT_STRIP_TRAILING_WHITESPACE
    COMMAND_ERROR_IS_FATAL ANY)
  if(DUCKDB_GIT_STATUS STREQUAL "")
    set(TELEMETRY_BUILD_ID "${DUCKDB_GIT_HASH}+browser-v1")
  else()
    set(TELEMETRY_BUILD_ID "${DUCKDB_GIT_HASH}.dirty+browser-v1")
  endif()
endif()
if(NOT TELEMETRY_BUILD_ID MATCHES "^[A-Za-z0-9._+:-]+$")
  message(FATAL_ERROR "DUCKDB_TELEMETRY_BUILD_ID contains unsupported characters")
endif()

set(BUILD_SHELL OFF CACHE BOOL "" FORCE)
set(BUILD_UNITTESTS OFF CACHE BOOL "" FORCE)
set(BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
set(BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(BUILD_QUENT_TELEMETRY ON CACHE BOOL "" FORCE)
set(BUILD_QUENT_BROWSER_TELEMETRY ON CACHE BOOL "" FORCE)
set(DISABLE_BUILTIN_EXTENSIONS ON CACHE BOOL "" FORCE)
set(DISABLE_EXTENSION_LOAD ON CACHE BOOL "" FORCE)
set(DISABLE_THREADS ON CACHE BOOL "" FORCE)
set(Rust_CARGO_TARGET wasm32-unknown-emscripten CACHE STRING "" FORCE)

add_compile_options(-fwasm-exceptions)
add_link_options(-fwasm-exceptions -sSUPPORT_LONGJMP=wasm)

add_subdirectory("${DUCKDB_ROOT}" duckdb)

add_executable(duckdb-browser duckdb_browser.cpp)
target_link_libraries(
  duckdb-browser
  PRIVATE
    duckdb_static
    duckdb_generated_extension_loader
    core_functions_extension)
target_include_directories(
  duckdb-browser
  PRIVATE
    "${DUCKDB_ROOT}/src/include"
    "${DUCKDB_ROOT}/third_party/fast_float"
    "${DUCKDB_ROOT}/third_party/fastpforlib"
    "${DUCKDB_ROOT}/third_party/fmt/include"
    "${DUCKDB_ROOT}/third_party/fsst"
    "${DUCKDB_ROOT}/third_party/hyperloglog"
    "${DUCKDB_ROOT}/third_party/mbedtls/include"
    "${DUCKDB_ROOT}/third_party/miniz"
    "${DUCKDB_ROOT}/third_party/pcg"
    "${DUCKDB_ROOT}/third_party/re2"
    "${DUCKDB_ROOT}/third_party/skiplist"
    "${DUCKDB_ROOT}/third_party/utf8proc/include"
    "${DUCKDB_ROOT}/third_party/yyjson/include"
    "${CMAKE_BINARY_DIR}/duckdb/codegen/include")
target_compile_features(duckdb-browser PRIVATE cxx_std_20)
target_compile_definitions(
  duckdb-browser
  PRIVATE
    DUCKDB_BROWSER_SCHEMA_HASH="${TELEMETRY_SCHEMA_HASH}"
    DUCKDB_BROWSER_BUILD_ID="${TELEMETRY_BUILD_ID}")
target_link_options(
  duckdb-browser
  PRIVATE
    -sALLOW_MEMORY_GROWTH=1
    -sENVIRONMENT=web,worker,node
    -sEXPORT_ES6=1
    -sEXPORT_NAME=createDuckDbBrowserModule
    -sMODULARIZE=1
    "-sSTACK_SIZE=${DUCKDB_BROWSER_STACK_SIZE}"
    -sSTACK_OVERFLOW_CHECK=1
    -sWASM_BIGINT=1
    "-sEXPORTED_FUNCTIONS=['_malloc','_free','_quent_browser_manifest_json','_quent_browser_open','_quent_browser_query','_quent_browser_result_json','_quent_browser_drain','_quent_browser_drain_status','_quent_browser_drain_ptr','_quent_browser_drain_len','_quent_browser_drain_count','_quent_browser_drain_min_ts','_quent_browser_drain_max_ts','_quent_browser_drain_dropped','_quent_browser_watermark','_quent_browser_query_ids_json','_quent_browser_context_id_json','_quent_browser_error_json','_quent_browser_reset']"
    "-sEXPORTED_RUNTIME_METHODS=['stringToNewUTF8','UTF8ToString','HEAPU8']")

set_target_properties(
  duckdb-browser
  PROPERTIES
    OUTPUT_NAME duckdb-browser
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/artifacts"
    SUFFIX ".js")
