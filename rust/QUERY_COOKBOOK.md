# DuckDB Quent query cookbook

These copy-paste workloads generate DuckDB telemetry for specific Quent UI
views. Run commands from the repository root. See
[INSTRUMENTATION.md](INSTRUMENTATION.md) for entity semantics.

## Prerequisites

Build DuckDB with telemetry enabled:

```bash
cd /path/to/duckdb
cmake -S . -B build/release \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_QUENT_TELEMETRY=ON
cmake --build build/release --target shell -j4
```

The examples use `build/release/duckdb`. Override it before running a workload
if the telemetry-enabled shell lives elsewhere:

```bash
export DUCKDB_BIN=build/release/duckdb
```

## Start the analyzer after a capture

Every workload below assigns its capture directory to `events_dir`. After the
DuckDB command exits, run this in the same shell:

```bash
cd /path/to/duckdb
cargo run --manifest-path rust/Cargo.toml \
    -p duckdb-telemetry-server --features ui -- \
    --output-dir "$events_dir" \
    --collector-address 127.0.0.1:7836 \
    --analyzer-address 127.0.0.1:8080
```

Open `http://127.0.0.1:8080`. Stop the server before loading another capture;
the analyzer caches an engine after first access.

Most command blocks execute settings and data preparation before the target
query. Select the final query named in each section, not a preceding `SET` or
`CREATE TABLE` statement.

## UI checklist

Open the selected query's Timeline page. Expand `local` for worker resources
and the Engine root for memory resources.

| UI selection | Expected meaning |
|---|---|
| `runnable-pipeline-tasks` + `pipeline_task` | Initial dispatch and partial-yield backlog |
| `thread-*` + `pipeline_task` | Pipeline task wall-time occupancy |
| `thread-*` + `operator_invocation` | Exact physical-operator calls |
| `temporary-spill` + `temporary_block_io` | Spill operations/s and buffer B/s |
| `temporary-reload` + `temporary_block_io` | Reload operations/s and buffer B/s |
| `buffer-pool-memory` + `memory_account` | Managed bytes by `MemoryTag` |
| `temporary-storage` + `memory_account` | Live evicted bytes by `MemoryTag` |
| `temporary-directory-storage` + `memory_account` | Accounted temporary-file bytes |
| Plan dataflow overlay | Published chunks, rows, and logical bytes/s |

Selecting a physical operator filters these views. A task is included when its
pipeline contains the selected operator. Operator invocations use exact
operator identity. Temporary I/O uses causal `trigger_operator_id`.
The three memory resources are database-wide and disappear under any operator
selection. A shared buffer pool includes activity from every attached database.

## Recommended generated workload: everything at once

This validated workload combines stored-table parallelism, partial yields,
joins, blocking aggregation, two windows, ordering, chunk flow, and forced
spill/reload in one target query.

```bash
cd /path/to/duckdb
DUCKDB_BIN=${DUCKDB_BIN:-build/release/duckdb}
run_dir=$(mktemp -d /tmp/duckdb-quent-combined.XXXXXX)
events_dir="$run_dir/events"
spill_dir="$run_dir/spill"
mkdir "$events_dir" "$spill_dir"

QUENT_EXPORTER=ndjson \
QUENT_OUTPUT_DIR="$events_dir" \
"$DUCKDB_BIN" "$run_dir/combined.duckdb" -c "
SET threads=4;
SET scheduler_process_partial=true;

CREATE TABLE customers AS
SELECT
    i::BIGINT AS customer_id,
    'region-' || (i % 16)::VARCHAR AS region,
    (i % 5)::INTEGER AS segment
FROM range(100000) t(i);

CREATE TABLE orders AS
SELECT
    i::BIGINT AS order_id,
    (i % 100000)::BIGINT AS customer_id,
    DATE '2020-01-01' + (i % 1460)::INTEGER AS order_date,
    (i % 4)::INTEGER AS status
FROM range(1000000) t(i);

CREATE TABLE lineitem AS
SELECT
    i::BIGINT AS line_id,
    (i % 1000000)::BIGINT AS order_id,
    (i % 100000)::BIGINT AS part_id,
    1 + (i % 20)::INTEGER AS quantity,
    100 + (i % 10000)::BIGINT AS unit_price,
    (i % 11)::INTEGER AS discount
FROM range(4000000) t(i);

SET memory_limit='128MB';
SET temp_directory='$spill_dir';
SET preserve_insertion_order=false;
SET debug_force_external=true;

WITH revenue_by_order AS MATERIALIZED (
    SELECT
        order_id,
        sum(quantity * unit_price * (100 - discount)) / 100.0 AS revenue,
        count(*) AS item_count,
        approx_count_distinct(part_id) AS distinct_parts
    FROM lineitem
    WHERE part_id % 7 IN (0, 1, 2)
    GROUP BY order_id
),
ranked AS MATERIALIZED (
    SELECT
        c.region,
        c.segment,
        o.order_date,
        r.order_id,
        r.revenue,
        r.item_count,
        r.distinct_parts,
        row_number() OVER (
            PARTITION BY c.region
            ORDER BY r.revenue DESC
        ) AS revenue_rank,
        sum(r.revenue) OVER (
            PARTITION BY c.region
            ORDER BY o.order_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS running_revenue
    FROM revenue_by_order r
    JOIN orders o USING (order_id)
    JOIN customers c USING (customer_id)
    WHERE o.status <> 3
)
SELECT
    region,
    date_trunc('month', order_date) AS month,
    count(*) AS order_count,
    sum(revenue) AS gross_revenue,
    avg(item_count) AS avg_items,
    max(running_revenue) AS peak_running_revenue
FROM ranked
WHERE revenue_rank <= 50000
GROUP BY region, month
ORDER BY month, gross_revenue DESC
LIMIT 12;
"

echo "Quent events: $events_dir"
echo "DuckDB spill directory: $spill_dir"
```

In one validated run, the target produced 23 tasks across four threads, 53
Ready yields, 17,230 operator invocations, 12,898 chunk publications, 16
spills, and 22 reloads in an approximately 38 MB capture. Counts vary with
build and scheduling.

Select the final `WITH revenue_by_order AS ...` query. Highlight `SEQ_SCAN`,
`HASH_GROUP_BY`, `HASH_JOIN`, and `WINDOW` in turn. This is the shortest path
to exercising every currently implemented runtime entity and resource.

## Generated workload 1: parallel pipeline

Purpose:

- several pipeline tasks and execution threads;
- partial task yields and runnable intervals;
- scan, filter, hash join, projection, hash aggregate, and order operators;
- chunk, row, and logical-byte plan-edge flow.

The target reads stored tables because DuckDB's `range()` table function is
forced single-threaded.

```bash
cd /path/to/duckdb
DUCKDB_BIN=${DUCKDB_BIN:-build/release/duckdb}
events_dir=$(mktemp -d /tmp/duckdb-quent-parallel.XXXXXX)

QUENT_EXPORTER=ndjson \
QUENT_OUTPUT_DIR="$events_dir" \
"$DUCKDB_BIN" -c "
SET threads=4;
SET scheduler_process_partial=true;
SET preserve_insertion_order=false;

CREATE TABLE fact AS
SELECT
    i::BIGINT AS id,
    (i % 10000)::INTEGER AS dimension_id,
    (i % 1024)::INTEGER AS group_id,
    (i % 97)::BIGINT AS measure
FROM range(8000000) t(i);

CREATE TABLE dimension AS
SELECT
    i::INTEGER AS dimension_id,
    ('region-' || (i % 16))::VARCHAR AS region
FROM range(10000) t(i);

SELECT
    d.region,
    f.group_id,
    count(*) AS rows,
    sum(f.measure) AS measure_sum
FROM fact f
JOIN dimension d USING (dimension_id)
WHERE f.id % 7 = 0
GROUP BY d.region, f.group_id
ORDER BY measure_sum DESC, d.region, f.group_id
LIMIT 200;
"

echo "Quent events: $events_dir"
```

Select the final `SELECT d.region ...` query. Select `SEQ_SCAN` to see several
tasks, then `HASH_JOIN` or `HASH_GROUP_BY` for exact invocation spans. The
number of active threads may be below four; `SET threads=4` is a limit.

## Generated workload 2: spill, reload, and memory occupancy

Purpose:

- `TemporaryBlockIo` spill and reload entities;
- `temporary-spill` and `temporary-reload` rate resources;
- buffer-pool, live temporary, and directory occupancy resources;
- causal task and `HASH_JOIN` attribution;
- memory-tag and stored-size attributes.

`debug_force_external` makes external execution reproducible. Do not use it
for ordinary performance measurements.

```bash
cd /path/to/duckdb
DUCKDB_BIN=${DUCKDB_BIN:-build/release/duckdb}
events_dir=$(mktemp -d /tmp/duckdb-quent-spill.XXXXXX)
spill_dir=$(mktemp -d /tmp/duckdb-spill.XXXXXX)

QUENT_EXPORTER=ndjson \
QUENT_OUTPUT_DIR="$events_dir" \
"$DUCKDB_BIN" -c "
SET threads=4;
SET scheduler_process_partial=true;
SET memory_limit='128MB';
SET temp_directory='$spill_dir';
SET max_temp_directory_size='1GB';
SET preserve_insertion_order=false;
SET debug_force_external=true;

SELECT count(*)
FROM range(10000000) build_side(i)
JOIN range(10000000) probe_side(i) USING (i);
"

echo "Quent events: $events_dir"
echo "DuckDB spill directory: $spill_dir"
```

Select the final join. Expand both temporary-I/O resources and choose
`temporary_block_io`. Selecting `HASH_JOIN` should preserve its causally
attributed I/O; selecting `RESULT_COLLECTOR` should produce an empty I/O
series.

At the Engine root, inspect `buffer-pool-memory`, `temporary-storage`, and
`temporary-directory-storage`. Choose `memory_account` to split occupancy by
tag. The first two show `HASH_TABLE` and `COLUMN_DATA`; the directory resource
has one `UNKNOWN` series. Clear the operator selection: these gauges are
database-wide and intentionally have no operator attribution.

In one validated run, the target took 0.519 seconds and the 58 MB capture held
24,960 memory-account events and 2,656 temporary-I/O events. Binned peaks were
approximately:

| Resource | Tag | Peak |
|---|---|---:|
| Buffer pool | `HASH_TABLE` | 121.24 MiB |
| Buffer pool | `COLUMN_DATA` | 102.27 MiB |
| Buffer pool | `ALLOCATOR` | 9.40 MiB |
| Live temporary storage | `HASH_TABLE` | 107.47 MiB |
| Live temporary storage | `COLUMN_DATA` | 69.44 MiB |
| Temporary directory | `UNKNOWN` | 176.91 MiB |

The values need not match. Buffer-pool charge, live evicted representations,
and DuckDB-accounted file extent are separate gauges. The first pair are split
by tag; only directory extent is charged directly against the 1 GB swap limit.

The source `range()` scans may remain serial. This workload targets temporary
I/O and memory pressure, not maximum scan parallelism. The temporary-I/O lanes
show operation and byte rates; the three memory lanes show byte occupancy.

## Generated workload 3: runtime failure

Purpose:

- task `Finalizing.success=false`;
- operator invocation `InvocationCompleted.success=false`;
- cleanup of an executing query after an operator exception.

The DuckDB command is expected to report a conversion error.

```bash
cd /path/to/duckdb
DUCKDB_BIN=${DUCKDB_BIN:-build/release/duckdb}
events_dir=$(mktemp -d /tmp/duckdb-quent-failure.XXXXXX)

QUENT_EXPORTER=ndjson \
QUENT_OUTPUT_DIR="$events_dir" \
"$DUCKDB_BIN" -c "
SET threads=4;
SET scheduler_process_partial=true;

CREATE TABLE cast_input AS
SELECT
    i::BIGINT AS id,
    CASE
        WHEN i = 3500000 THEN 'not-an-integer'
        ELSE i::VARCHAR
    END AS value
FROM range(5000000) t(i);

SELECT sum(value::BIGINT)
FROM cast_input;
"

echo "Quent events: $events_dir"
```

Select `SELECT sum(value::BIGINT) ...`. Inspect the terminal states of its task
and operator invocation entities. This workload does not exercise temporary
I/O.

## TPC-H data

The following commands default to the discovered SF10 partitioned Snappy data:

```text
/data/tpch/sf10/p16/snappy
```

Override it with another compatible TPC-H Parquet root, for example:

```bash
export TPCH_ROOT=/data/tpch/sf1/p16/snappy
```

Each table directory must contain `*.parquet` files with standard TPC-H column
names.

## TPC-H workload 1: scan and aggregate

This is TPC-H Q1's aggregation shape.

Purpose:

- parallel partitioned Parquet scan;
- filter, projection, aggregate, and order invocations;
- task and chunk-flow timelines without intentionally forcing spill.

```bash
cd /path/to/duckdb
DUCKDB_BIN=${DUCKDB_BIN:-build/release/duckdb}
tpch_root=${TPCH_ROOT:-/data/tpch/sf10/p16/snappy}
events_dir=$(mktemp -d /tmp/duckdb-quent-tpch-q1.XXXXXX)

QUENT_EXPORTER=ndjson \
QUENT_OUTPUT_DIR="$events_dir" \
"$DUCKDB_BIN" -c "
SET threads=4;
SET scheduler_process_partial=true;

SELECT
    l_returnflag,
    l_linestatus,
    sum(l_quantity) AS sum_qty,
    sum(l_extendedprice) AS sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
    avg(l_quantity) AS avg_qty,
    avg(l_extendedprice) AS avg_price,
    avg(l_discount) AS avg_disc,
    count(*) AS count_order
FROM read_parquet('$tpch_root/lineitem/*.parquet')
WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90 days'
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus;
"

echo "Quent events: $events_dir"
```

Select the final query and highlight `READ_PARQUET` or `HASH_GROUP_BY`. This is
the clearest workload for comparing scan task distribution with edge row and
logical-byte rates. One SF10 validation used four execution threads and 310
tasks and generated approximately 133 MB of events.

## TPC-H workload 2: join, aggregate, and window

This extends TPC-H Q9's profit calculation with two window functions.

Purpose:

- six partitioned Parquet inputs;
- several hash joins and pipelines;
- aggregate, window, and order operators;
- task and operator filtering;
- a nontrivial plan dataflow overlay.

```bash
cd /path/to/duckdb
DUCKDB_BIN=${DUCKDB_BIN:-build/release/duckdb}
tpch_root=${TPCH_ROOT:-/data/tpch/sf10/p16/snappy}
events_dir=$(mktemp -d /tmp/duckdb-quent-tpch-q9.XXXXXX)

QUENT_EXPORTER=ndjson \
QUENT_OUTPUT_DIR="$events_dir" \
"$DUCKDB_BIN" -c "
SET threads=4;
SET scheduler_process_partial=true;
SET preserve_insertion_order=false;

WITH
li AS (
    SELECT * FROM read_parquet('$tpch_root/lineitem/*.parquet')
),
o AS (
    SELECT * FROM read_parquet('$tpch_root/orders/*.parquet')
),
p AS (
    SELECT * FROM read_parquet('$tpch_root/part/*.parquet')
),
ps AS (
    SELECT * FROM read_parquet('$tpch_root/partsupp/*.parquet')
),
s AS (
    SELECT * FROM read_parquet('$tpch_root/supplier/*.parquet')
),
n AS (
    SELECT * FROM read_parquet('$tpch_root/nation/*.parquet')
),
profit AS (
    SELECT
        n_name AS nation,
        extract(year FROM o_orderdate) AS order_year,
        l_extendedprice * (1 - l_discount)
            - ps_supplycost * l_quantity AS amount
    FROM li
    JOIN p
      ON p_partkey = l_partkey
    JOIN ps
      ON ps_partkey = l_partkey
     AND ps_suppkey = l_suppkey
    JOIN s
      ON s_suppkey = l_suppkey
    JOIN n
      ON n_nationkey = s_nationkey
    JOIN o
      ON o_orderkey = l_orderkey
    WHERE p_name LIKE '%green%'
      AND l_shipdate >= DATE '1994-01-01'
      AND l_shipdate < DATE '1998-01-01'
),
yearly AS (
    SELECT
        nation,
        order_year,
        sum(amount) AS profit,
        count(*) AS line_count,
        avg(amount) AS avg_amount
    FROM profit
    GROUP BY nation, order_year
)
SELECT
    nation,
    order_year,
    profit,
    line_count,
    avg_amount,
    dense_rank() OVER (
        PARTITION BY order_year
        ORDER BY profit DESC
    ) AS yearly_rank,
    sum(profit) OVER (
        PARTITION BY nation
        ORDER BY order_year
        ROWS UNBOUNDED PRECEDING
    ) AS cumulative_profit
FROM yearly
ORDER BY order_year DESC, profit DESC, nation;
"

echo "Quent events: $events_dir"
```

Select the final `WITH li AS ...` query. Start with the worker's execution-thread
group, then select individual `HASH_JOIN` operators. Compare their exact
invocations, containing tasks, and dataflow inputs. Window and final ordering
work may be short because the aggregate has low cardinality.

## TPC-H workload 3: all runtime entities

This migration-validation workload combines a partitioned scan, join, blocking
aggregation, two windows, and forced external execution. Msgpack limits capture
size and import overhead.

```bash
cd /path/to/duckdb
DUCKDB_BIN=${DUCKDB_BIN:-build/release/duckdb}
tpch_root=${TPCH_ROOT:-/data/tpch/sf10/p16/snappy}
events_dir=$(mktemp -d /tmp/duckdb-quent-tpch-all.XXXXXX)
spill_dir=$(mktemp -d /tmp/duckdb-tpch-all-spill.XXXXXX)

QUENT_EXPORTER=msgpack \
QUENT_OUTPUT_DIR="$events_dir" \
"$DUCKDB_BIN" -c "
SET threads=4;
SET scheduler_process_partial=true;
SET memory_limit='512MB';
SET temp_directory='$spill_dir';
SET preserve_insertion_order=false;
SET debug_force_external=true;

WITH revenue AS (
    SELECT
        l_orderkey,
        o_custkey,
        o_orderdate,
        sum(l_extendedprice * (1 - l_discount)) AS revenue,
        count(*) AS line_count
    FROM read_parquet('$tpch_root/lineitem/*.parquet') l
    JOIN read_parquet('$tpch_root/orders/*.parquet') o
      ON o_orderkey = l_orderkey
    GROUP BY l_orderkey, o_custkey, o_orderdate
),
ranked AS (
    SELECT
        *,
        dense_rank() OVER (
            PARTITION BY year(o_orderdate)
            ORDER BY revenue DESC
        ) AS year_rank,
        sum(revenue) OVER (
            PARTITION BY o_custkey
            ORDER BY o_orderdate, l_orderkey
            ROWS UNBOUNDED PRECEDING
        ) AS customer_running_revenue
    FROM revenue
)
SELECT
    count(*) AS orders,
    round(sum(revenue), 2) AS total_revenue,
    max(year_rank) AS largest_rank,
    round(max(customer_running_revenue), 2) AS largest_running_revenue
FROM ranked;
"

echo "Quent events: $events_dir"
echo "DuckDB spill directory: $spill_dir"
```

Select the final `WITH revenue AS ...` query. Inspect `READ_PARQUET`,
`HASH_JOIN`, `HASH_GROUP_BY`, and `WINDOW`, then compare them with the spill and
reload resources. A pre-migration SF10 capture contained 33 tasks, 140,718
chunk publications, 215,988 operator invocations, and 28,742 temporary-I/O
operations. Treat these as regression context; counts vary by build and
scheduling.

## Event-volume guidance

Runtime telemetry is deliberately fine-grained. One vectorized operator call
creates an `OperatorInvocation`; every nonempty plan-edge output creates a
`ChunkTransfer`; every temporary operation creates four events. Each managed
memory charge, spill, reload, or deletion also advances an absolute
`MemoryAccount` gauge. Memory events can therefore be a material share of a
low-memory capture.

For a smaller first run:

```bash
export TPCH_ROOT=/data/tpch/sf1/p16/snappy
```

For the SF10 examples, allow several hundred megabytes for event output. A
join/window capture with forced external execution can exceed 500 MB. Avoid
SF100 or SF300 until the capture and analyzer cost is acceptable.

## Troubleshooting

### No telemetry files

Confirm that the selected binary was configured with
`BUILD_QUENT_TELEMETRY=ON` and that `QUENT_EXPORTER=ndjson` is present on the
DuckDB process.

### Only one execution thread

Thread count is a maximum. Direct `range()` scans are forced single-threaded.
Use the stored-table workload or partitioned Parquet workloads. Inspect usage
for the selected query; a thread resource may have been created by an earlier
statement without being used by the target query.

### No temporary I/O

Use the spill workloads with `debug_force_external=true`, a writable
`temp_directory`, and the documented memory limit. Remove
`debug_force_external` for representative performance experiments.

### Analyzer shows stale data

Stop and restart the analyzer after adding events. Start it after the DuckDB
process exits for the most stable filesystem capture.
