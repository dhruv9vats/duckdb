# Browser query walkthrough

Run each SQL block separately in the demo. Start with **Reset database**, then
run setup once. Tables survive subsequent runs until reset or page reload.
Select a capture, then explore Quent's **Timeline**, **Operators**, and
**Entities** tabs. Use **Hide SQL** for more chart space.

These examples use generated data; no download or extension is required.
All 12 SQL blocks were executed in order against the demo's WASM producer on
2026-09-21; each succeeded and emitted telemetry without dropped events.
Results stay small because the preview limit does not bound query memory.
Reset and repeat setup between groups of examples if capture limits are reached.
The demo retains at most eight revisions; its session and snapshot budgets can
require a reset sooner.

## What can appear

| Entity/resource | What to inspect |
|---|---|
| Engine, worker, query group, query | Profile navigation; the worker is a backend, not a CPU thread. |
| Plan, operators, ports | Query graph and Operators tab; plans can change with optimization. |
| `pipeline_task` | Entities: task lifecycle; Timeline: task queue and execution resources. |
| `operator_invocation` | Entities: source, execute, sink, and other recorded operator calls. |
| `chunk_transfer` | Entities and graph data flow: batches passed between operators, not individual rows. |
| `memory_account` | Entities and buffer-pool Timeline: tracked allocation/occupancy, not total browser RAM. |
| `temporary_block_io` | Not demonstrated by this browser build; use native DuckDB with spilling. |

Choose an entity type explicitly when **All types** is dominated by longer-lived
memory accounts. Expand resource groups, zoom into execution, and lower the
long-entity threshold if short tasks are hidden. An empty threshold-filtered
row does not imply no events occurred.

The browser executes on one thread. These queries do not demonstrate parallel
scheduler contention, disk bandwidth, or network latency. Query `Planning`
measures plan telemetry emission, not SQL parsing or optimization.

## 1. Setup: table creation and materialization

```sql
CREATE TABLE customers AS
SELECT i AS customer_id, i % 8 AS region
FROM range(1000) t(i);

CREATE TABLE orders AS
SELECT i AS order_id,
       i % 1000 AS customer_id,
       i % 17 AS category,
       1 + i % 100 AS amount,
       DATE '2024-01-01' + CAST(i % 365 AS INTEGER) AS order_date
FROM range(20000) t(i);

SELECT count(*) AS orders, sum(amount) AS total_amount FROM orders;
```

Expected: `20000`, `1010000`. One Run can contain multiple queries; use Quent's
query selector to inspect each CREATE separately. The last SELECT opens first.

## 2. Scan, filter, projection, aggregate

```sql
SELECT count(*) AS rows_kept, sum(amount * 2) AS doubled_total
FROM orders
WHERE category = 3;
```

Inspect table scan, pushed-down filtering, projection, and aggregate. Filters
may be fused into scans rather than represented by a separate FILTER node.
Follow invocation and chunk-transfer entities through the plan.

## 3. Equality join and grouped aggregation

```sql
SELECT c.region, count(*) AS orders, sum(o.amount) AS revenue
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
GROUP BY c.region
ORDER BY c.region;
```

Expected: eight regions, each with 2500 orders. Usually a hash join: inspect
build/probe work, separate pipelines, and buffer-pool occupancy. Invocation
phases describe instrumented calls, not a promise of separate build/probe FSMs.

## 4. Left join with unmatched rows

```sql
SELECT count(*) AS rows_out, count(c.customer_id) AS matched
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id AND c.region = 0;
```

Expected: `20000`, `2500`. Compare input/output row attributes with the inner
join. The join must preserve unmatched left rows.

## 5. Semi join and anti join

```sql
SELECT count(*) AS matching_orders
FROM orders o
SEMI JOIN customers c ON o.customer_id = c.customer_id AND c.region = 0;
```

Expected: `2500`. Semi joins test existence without duplicating matching rows.

```sql
SELECT count(*) AS nonmatching_orders
FROM orders o
ANTI JOIN customers c ON o.customer_id = c.customer_id AND c.region = 0;
```

Expected: `17500`. Compare join types and output chunk sizes.

## 6. Inequality join

```sql
SELECT count(*) AS pairs
FROM range(200) a(i)
JOIN range(200) b(j) ON a.i < b.j;
```

Expected: `19900`. Inspect the selected physical join algorithm; do not assume
an equality/hash-join plan. Keep the ranges small: output cardinality is quadratic.

## 7. Full sort, with bounded output

```sql
SELECT count(*) AS rows_sorted, sum(amount) AS total
FROM (
    SELECT order_id, amount FROM orders ORDER BY amount DESC, order_id
) sorted;
```

Expected: `20000`, `1010000`. Inspect ORDER_BY and its sink/source work. Confirm
the sort exists in the displayed plan; SQL syntax alone is not proof of work.

## 8. Top-N

```sql
SELECT order_id, amount
FROM orders
ORDER BY amount DESC, order_id
LIMIT 10;
```

Expected: ten rows, all with amount `100`. Compare TOP_N with the full sort:
operator lifetimes, transferred rows, and memory-account timelines.

## 9. Window functions whose results are consumed

```sql
SELECT count(*) AS rows_seen, max(running_amount) AS largest_running_amount,
       max(amount_rank) AS largest_rank
FROM (
    SELECT sum(amount) OVER (
               PARTITION BY customer_id ORDER BY order_date, order_id
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
           ) AS running_amount,
           dense_rank() OVER (
               PARTITION BY category ORDER BY amount DESC
           ) AS amount_rank
    FROM orders
) ranked;
```

Expected: `20000`, `2000`, `100`. Different window specifications can create
multiple WINDOW operators. Inspect their buffering and invocation sequences.
Consuming the window outputs prevents an unused-output example from being
optimized away.

## 10. Join, aggregate, then window

```sql
WITH totals AS (
    SELECT c.region, o.category, sum(o.amount) AS revenue
    FROM orders o
    JOIN customers c ON o.customer_id = c.customer_id
    GROUP BY c.region, o.category
), ranked AS (
    SELECT *, dense_rank() OVER (
        PARTITION BY region ORDER BY revenue DESC
    ) AS revenue_rank
    FROM totals
)
SELECT region, category, revenue, revenue_rank
FROM ranked
WHERE revenue_rank <= 2
ORDER BY region, revenue_rank, category;
```

Trace chunk transfers across join, aggregate, window, filter, and ordering.
This is the compact end-to-end demo. CTEs do not guarantee materialization;
inspect the actual graph.

## External data: current limits

This demo uses a custom instrumented Emscripten build, not the standard
DuckDB-Wasm JavaScript client. It currently has:

- No file picker, upload API, URL registration, or host-file mapping.
- Extension loading disabled; only core functions linked, not Parquet/httpfs.
- No browser HTTP filesystem integration.

Therefore `read_parquet('https://…')`, `INSTALL httpfs`, and paths on your
computer do **not** provide an external-data workflow here. Serving a file next
to the page does not automatically make it visible to DuckDB.

For small external datasets, convert them to SQL and paste `CREATE TABLE` plus
`INSERT ... VALUES` statements into the editor. Review SQL before executing it;
escape string quotes by doubling them. Example:

```sql
CREATE TABLE imported_sales (customer_id BIGINT, amount BIGINT);
INSERT INTO imported_sales VALUES (0, 125), (1, 250), (0, 75);
SELECT c.region, sum(s.amount) AS revenue
FROM imported_sales s
JOIN customers c USING (customer_id)
GROUP BY c.region
ORDER BY c.region;
```

Expected: region `0` → `200`; region `1` → `250`.

Proper file loading would need a bounded upload/fetch path, transfer to the
DuckDB worker, a worker-owned virtual-filesystem registration API, and cleanup.
CSV could then use the core reader; Parquet needs its extension compiled into
this instrumented build. Remote downloads also require the source server's
CORS permission. These capabilities are not implemented by this guide.

See [instrumentation semantics](../../rust/INSTRUMENTATION.md) for precise
entity meanings and [runtime limits](README.md#runtime-limits) before scaling up.
