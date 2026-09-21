# Vendored Quent crates

Source: `rapidsai/quent` `4a091722f1a5b7a93e5883b82b838eea43f28c3a`.

`quent-time` supplies an Emscripten epoch-preserving monotonic timestamp.
`quent-instrumentation` synchronously forwards browser callback events; it
does not create Tokio tasks or network exporters on wasm32.
`quent-query-engine-analyzer` separates native I/O from the portable analyzer
so the same API runs in WebAssembly. All retain their upstream SPDX
Apache-2.0 notices.
