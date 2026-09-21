import { createReadStream, statSync } from 'node:fs';
import { createServer } from 'node:http';
import { extname, join, normalize } from 'node:path';

const port = Number(process.env.PORT ?? process.env.PLAYWRIGHT_PORT ?? 4173);
const root = normalize(join(import.meta.dirname, '..', 'dist'));
const prefix = process.env.QUENT_BASE_PATH ?? '/duckdb-quent/';
const contentTypes = {
  '.css': 'text/css; charset=utf-8',
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.wasm': 'application/wasm',
};

createServer((request, response) => {
  const path = new URL(request.url ?? '/', 'http://localhost').pathname;
  const relative = path.startsWith(prefix) ? path.slice(prefix.length) : path.slice(1);
  const candidate = normalize(join(root, relative || 'index.html'));
  if (!candidate.startsWith(root)) {
    response.writeHead(403).end();
    return;
  }

  let file = candidate;
  try {
    if (statSync(file).isDirectory()) {
      file = join(file, 'index.html');
    }
  } catch {
    file = join(root, 'index.html');
  }
  response.setHeader('Content-Type', contentTypes[extname(file)] ?? 'application/octet-stream');
  createReadStream(file).pipe(response);
}).listen(port, '127.0.0.1');
