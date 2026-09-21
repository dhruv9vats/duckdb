import assert from 'node:assert/strict';
import { pathToFileURL } from 'node:url';

const PROTOCOL_VERSION = 1;
const SCHEMA_HASH = 'd5dfec3faf35450fad26e546879bac257908f516f4d4539866f62acb0072ad42';

const modulePath = process.argv[2];
if (!modulePath) {
  throw new Error('usage: test-producer.mjs MODULE_JS');
}

const createModule = (await import(pathToFileURL(modulePath))).default;
const engine = await createModule();
const manifest = JSON.parse(engine.UTF8ToString(engine._quent_browser_manifest_json()));
assert.equal(manifest.protocol_version, PROTOCOL_VERSION);
assert.equal(manifest.schema_hash, SCHEMA_HASH);
if (process.env.DUCKDB_TELEMETRY_BUILD_ID) {
  assert.equal(manifest.build_id, process.env.DUCKDB_TELEMETRY_BUILD_ID);
} else {
  assert.match(manifest.build_id, /\+browser-v1$/);
}
assert.equal(engine._quent_browser_open(), 0);

function queryStatus(sql) {
  const pointer = engine.stringToNewUTF8(sql);
  try {
    return engine._quent_browser_query(pointer, 10);
  } finally {
    engine._free(pointer);
  }
}

function query(sql) {
  assert.equal(queryStatus(sql), 0);
  return JSON.parse(engine.UTF8ToString(engine._quent_browser_result_json()));
}

function drain() {
  let eventCount = 0;
  while (true) {
    engine._quent_browser_drain(4n * 1024n * 1024n);
    const status = engine._quent_browser_drain_status();
    if (status === 0) {
      break;
    }
    assert.equal(status, 1);
    const length = engine._quent_browser_drain_len();
    assert.ok(length > 0);
    const pointer = engine._quent_browser_drain_ptr();
    assert.equal(engine.HEAPU8.slice(pointer, pointer + length).length, length);
    assert.equal(engine._quent_browser_drain_dropped(), 0n);
    eventCount += engine._quent_browser_drain_count();
  }
  assert.ok(eventCount > 0);
}

assert.deepEqual(query("SELECT 'a' || chr(1) || 'é' AS value").rows, [['a\u0001é']]);
assert.ok(JSON.parse(engine.UTF8ToString(engine._quent_browser_query_ids_json())).length >= 1);
assert.equal(queryStatus('SELECT 42'), 1);
drain();
assert.deepEqual(query('SELECT count(*) AS count, sum(i) AS total FROM range(10000) t(i)').rows, [
  ['10000', '49995000'],
]);

const queryIds = JSON.parse(engine.UTF8ToString(engine._quent_browser_query_ids_json()));
assert.equal(queryIds.length, 1);
drain();
assert.ok(engine._quent_browser_watermark() > 0n);

assert.deepEqual(
  query(
    'CREATE TABLE t AS SELECT i, i % 31 AS k FROM range(250000) t(i); ' +
      'SELECT count(*), sum(i) FROM t',
  ).rows,
  [['250000', '31249875000']],
);
drain();
assert.deepEqual(
  query(
    'SELECT count(*) AS n, sum(total) AS total, max(r) AS max_rank FROM (' +
      'SELECT k, sum(i) AS total, dense_rank() OVER (ORDER BY sum(i)) AS r FROM t GROUP BY k)',
  ).rows,
  [['31', '31249875000', '31']],
);
drain();
assert.deepEqual(
  query('SELECT count(*) FROM range(1000) a(i) JOIN range(500) b(j) ON i = j').rows,
  [['500']],
);
drain();

console.log(`producer smoke passed (${queryIds.length} queries)`);
