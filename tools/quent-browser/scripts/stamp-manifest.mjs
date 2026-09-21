import { createHash } from 'node:crypto';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';

const root = join(import.meta.dirname, '..', 'public');
const manifestPath = join(root, 'build-manifest.json');
const appRoot = join(root, '..');
const repoRoot = join(appRoot, '..', '..');
const templatePath = join(appRoot, 'build-manifest.template.json');
const schemaPath = join(repoRoot, 'rust', 'crates', 'telemetry', 'model', 'model.yaml');
const manifest = JSON.parse(await readFile(templatePath, 'utf8'));
const assetKeys = [
  'producer_worker_url',
  'duckdb_module_js_url',
  'duckdb_wasm_url',
  'analyzer_worker_url',
  'analyzer_wasm_js_url',
  'analyzer_wasm_url',
];

const sourceRevision = process.env.DUCKDB_TELEMETRY_BUILD_ID ?? await gitSourceId(repoRoot);
manifest.schema_hash = createHash('sha256').update(await readFile(schemaPath)).digest('hex');
manifest.build_id = process.env.DUCKDB_TELEMETRY_BUILD_ID ?? sourceRevision;

manifest.content_sha256 = {};
for (const key of assetKeys) {
  const path = manifest[key];
  if (typeof path !== 'string') {
    throw new Error(`Missing manifest asset: ${key}`);
  }
  const data = await readFile(join(root, path));
  manifest.content_sha256[path] = createHash('sha256').update(data).digest('hex');
}
await writeFile(manifestPath, `${JSON.stringify(manifest, null, 2)}\n`);

async function gitSourceId(path) {
  const run = promisify(execFile);
  const revision = await run('git', ['-C', path, 'rev-parse', 'HEAD']);
  const status = await run('git', ['-C', path, 'status', '--porcelain', '--untracked-files=normal']);
  const dirty = status.stdout.trim() ? '.dirty' : '';
  return `${revision.stdout.trim()}${dirty}+browser-v1`;
}
