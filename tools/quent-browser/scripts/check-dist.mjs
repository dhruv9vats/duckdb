import { createHash } from 'node:crypto';
import { readFile, stat } from 'node:fs/promises';
import { dirname, isAbsolute, relative, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

const HTML_ENTRIES = ['index.html', 'iframe/index.html'];
const STATIC_FILES = ['iframe/logo.svg'];
const MANIFEST_PATH = 'build-manifest.json';
const MANIFEST_ASSETS = [
  'producer_worker_url',
  'duckdb_module_js_url',
  'duckdb_wasm_url',
  'analyzer_worker_url',
  'analyzer_wasm_js_url',
  'analyzer_wasm_url',
];
const SHA256_LENGTH = 64;
const WEB_ASSET = /(?:src|href)=["']([^"']+\.(?:css|js)(?:[?#][^"']*)?)["']/g;

export async function checkDist(inputRoot) {
  const root = resolve(inputRoot);
  const manifest = JSON.parse(await readFile(resolve(root, MANIFEST_PATH), 'utf8'));

  await Promise.all(STATIC_FILES.map(path => assertFile(resolve(root, path), path)));
  await checkHtml(root);
  await checkManifest(root, manifest);
}

async function checkHtml(root) {
  for (const entry of HTML_ENTRIES) {
    const htmlPath = resolve(root, entry);
    const html = await readFile(htmlPath, 'utf8');
    const references = [...html.matchAll(WEB_ASSET)].map(match => match[1]);

    if (!references.some(reference => cleanReference(reference).endsWith('.js'))) {
      throw new Error(`${entry} does not reference a JavaScript entry`);
    }

    for (const reference of references) {
      if (isExternal(reference) || reference.startsWith('/')) {
        throw new Error(`${entry} contains a non-relative asset URL: ${reference}`);
      }

      const assetPath = resolve(dirname(htmlPath), cleanReference(reference));
      assertInside(root, assetPath, reference);
      await assertFile(assetPath, `${entry} asset ${reference}`);
    }
  }
}

async function checkManifest(root, manifest) {
  if (!manifest.content_sha256 || typeof manifest.content_sha256 !== 'object') {
    throw new Error('build manifest has no content_sha256 map');
  }

  for (const field of MANIFEST_ASSETS) {
    const reference = manifest[field];
    if (typeof reference !== 'string' || isAbsolute(reference) || reference.startsWith('/')) {
      throw new Error(`build manifest has invalid ${field}`);
    }

    const assetPath = resolve(root, reference);
    assertInside(root, assetPath, reference);
    const data = await readFile(assetPath);
    const expected = manifest.content_sha256[reference];
    const actual = createHash('sha256').update(data).digest('hex');

    if (typeof expected !== 'string' || expected.length !== SHA256_LENGTH || expected !== actual) {
      throw new Error(`build manifest hash mismatch: ${reference}`);
    }
  }
}

function cleanReference(reference) {
  return decodeURIComponent(reference.split(/[?#]/, 1)[0]);
}

function isExternal(reference) {
  return /^[a-z][a-z\d+.-]*:/i.test(reference) || reference.startsWith('//');
}

function assertInside(root, target, reference) {
  const path = relative(root, target);
  if (path === '..' || path.startsWith(`..${process.platform === 'win32' ? '\\' : '/'}`)) {
    throw new Error(`asset escapes dist: ${reference}`);
  }
}

async function assertFile(path, label) {
  if (!(await stat(path)).isFile()) {
    throw new Error(`${label} is not a file`);
  }
}

const invokedPath = process.argv[1] ? pathToFileURL(resolve(process.argv[1])).href : undefined;
if (invokedPath === import.meta.url) {
  await checkDist(process.argv[2] ?? resolve(import.meta.dirname, '..', 'dist'));
}
