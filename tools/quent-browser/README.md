# DuckDB + Quent browser

This static application runs the instrumented DuckDB engine and telemetry
analyzer in dedicated workers. A same-origin iframe renders the pinned, full
Quent application beside the SQL shell. No query or capture leaves the browser.

The iframe uses Quent's route tree, navigation, theme, plan, timeline, entity,
and data-flow pages. A transferred `MessagePort` exposes an allowlisted
`ApiClient`; there is no HTTP API or global `fetch` override. Captures are
memory-only. A full-page reload loses them. Copy Link is disabled because a URL
cannot reconstruct an in-memory capture. Reloading only the iframe preserves a
matching query route while its parent session remains alive.

## Build

Requirements: Node 24.11, pnpm 11.19, Rust 1.98.1 with the browser targets,
`wasm-bindgen-cli` 0.2.127, and Emscripten 6.0.9.

```bash
rustup component add rust-src --toolchain 1.98.1
rustup target add wasm32-unknown-unknown wasm32-unknown-emscripten --toolchain 1.98.1
```

```bash
cd tools/quent-browser
bash scripts/prepare-quent.sh
pnpm install --frozen-lockfile
pnpm analyzer:build
bash scripts/build-producer.sh
pnpm test
pnpm build
```

`pnpm build` builds both the shell and full Quent iframe. It writes
`dist/index.html`, `dist/iframe/index.html`, and their shared hashed assets.
Asset URLs are relative, so the directory can be served at `/` or a project
subpath such as `/duckdb/`. Quent routes use the URL hash. Serve WASM as
`application/wasm`; do not open `index.html` directly from disk.
The build derives schema and source identity, hashes all six runtime assets,
and uses those hashes to version worker and WASM URLs.

After building, serve the verified artifact locally:

```bash
PORT=4190 node tests/static-server.mjs
```

Open `http://127.0.0.1:4190/duckdb-quent/`.

See the [query walkthrough](QUERIES.md) for joins, windows, entity examples,
and current external-data limits.

## Test

```bash
pnpm test
QUENT_REAL_ANALYZER=1 QUENT_REAL_PRODUCER=1 pnpm test:e2e
```

Set `PLAYWRIGHT_CHROMIUM_EXECUTABLE`, `PLAYWRIGHT_FIREFOX_EXECUTABLE`, or
`PLAYWRIGHT_WEBKIT_EXECUTABLE` when browsers are managed outside Playwright.
The end-to-end server exercises the build at `/duckdb-quent/`.

Use `?fixture=1` only for deterministic UI tests. The page keeps a visible
fixture badge because that mode does not execute DuckDB.

## GitHub Pages

Pushing `quent` runs the real producer/analyzer gates and deploys the verified
artifact to `https://dhruv9vats.github.io/duckdb/`. A manual workflow run builds
any selected ref, but deploys only from `quent` with `deploy=true`.

Once, set **Settings → Pages → Build and deployment → Source** to **GitHub
Actions**. Configure the `github-pages` environment to allow only `quent`.
Required reviewers may intentionally hold the deploy job.

See the [implementation plan](../../rust/FULL_QUENT_BROWSER_PLAN.md) for
ownership, protocol, acceptance, and failure contracts.

## Runtime limits

- 4 MiB maximum telemetry batch.
- 64 MiB maximum encoded capture and session.
- 512 MiB estimated snapshot budget at ten times encoded source bytes.
- At most eight retained revisions; budget pressure may require earlier reset.
- One active query in a single-threaded worker.
- No browser spill-I/O telemetry.
- NVTX methods return no catalog or viewport data.

SQL results may appear before telemetry publication. Each immutable telemetry
revision replays the bounded session. The snapshot estimate is admission
accounting, not an allocator or RSS cap. Forced cancellation or a session or
snapshot limit requires reset; forced cancellation also recreates the engine.
The row limit bounds the displayed preview only. DuckDB materializes results
before serialization, so use aggregates for memory-safe demonstrations.
