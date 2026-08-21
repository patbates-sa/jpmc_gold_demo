# Gold+ join-and-recluster demo — Databricks source tables

Generates the partitioned Delta tables for the JPMC session on Friday, August 21,
8:00–9:00am ET, so the reference pattern in
`gold_plus_join_recluster_employee_department.md` can be demonstrated live rather
than described.

Synthetic only. Nothing here derives from JPMC data. The schema follows Chetan
Kedia's own example, which he chose because *"everybody has the data, so it will
be much easier."*

## The shape being reproduced

| Table | Partitioned on | Rows at default scale |
|---|---|---|
| `gold_employee` | `department_number`, `employee_id_bucket` | 10,000 |
| `gold_department` | `department_number` | 24 |
| Join key | `department_name` | present in both, partitioned in neither |

That last row is the entire point. The join key is not a partition column on
either side, so the ingest-optimized layout cannot be used for partition pruning
or a co-located join. Do not "fix" the partitioning — the demo depends on it.

One substitution worth knowing before someone asks. Kedia's spec partitions the
large table on `employee_id`, which at billions of rows would mean one partition
per employee — unusable on any engine. `employee_id_bucket` (`employee_id % n`)
stands in for it and preserves the only property the demo needs: the large table
is physically laid out on key columns, and `department_name` is not one of them.

## Contents

```
render_sql.py                 renders the three SQL scripts
sql/01_create_and_generate.sql  creates and populates both source tables
sql/02_incremental_batch.sql    lands new hires + transfers between dbt runs
sql/03_verify.sql               ten pre-flight checks, expected answers inline
dbt/models/                     the three Gold+ variants, sources, and tests
```

## Running it

No local Spark, no upload, no cluster libraries. The SQL generates the data in
place on Databricks using `range()` and `hash()`, so you paste it into a notebook
or the SQL editor and run it.

The demo lands in the `jpmc_cib_demo` catalog rather than `main`, so nothing here
touches a shared namespace and teardown is a single `DROP CATALOG ... CASCADE`.
Create it once, by hand — the renderer emits `CREATE SCHEMA` but not
`CREATE CATALOG`, so pointing `--catalog` at a catalog that does not exist yet
fails on the first statement of script 01:

```sql
CREATE CATALOG IF NOT EXISTS jpmc_cib_demo
  COMMENT 'Gold+ join-and-recluster demo. Synthetic. Disposable.';
```

That needs `CREATE CATALOG` on the metastore, which is a metastore-admin grant
rather than a workspace one. If you do not have it, fall back to `--catalog main`
and everything below still works. Omit `MANAGED LOCATION` unless a storage
policy requires a specific bucket; without it the catalog inherits the
metastore's default root, which is fine for a demo.

Then render:

```bash
python3 render_sql.py --catalog jpmc_cib_demo --schema gold_demo
python3 render_sql.py --catalog jpmc_cib_demo --schema gold_demo --employees 5000000 --buckets 32
python3 render_sql.py                                   # defaults: main.gold_demo, 10,000 employees
```

Re-render rather than hand-editing the SQL. The catalog and schema are
interpolated into all three scripts, so changing one `CREATE SCHEMA` line by hand
leaves the table references pointing somewhere else.

Then, on Databricks:

1. Run `sql/01_create_and_generate.sql`.
2. Run `sql/03_verify.sql` and confirm every check.
3. Point a dbt project at the `dbt/models/` directory and `dbt build`.

`dbt/models/sources.yml` is pinned to `jpmc_cib_demo` / `gold_demo` to match. Set
`catalog:` and `schema:` in your `profiles.yml` target to the same pair so the
Gold+ models are written beside their sources — in `dbt-databricks`, `catalog:`
is the Unity Catalog catalog and `schema:` is the schema. If you rendered with
different values, change them in both places.

Generation at default scale takes seconds. At five million rows it is still under
a minute on a small warehouse.

Everything is deterministic — the data comes from `hash()` on `employee_id` and a
fixed `--asof` anchor, so re-running produces identical tables. Rebuild the night
before and the numbers you rehearsed still hold.

## Defaults, and why they are what they are

10,000 employees across 24 departments with 2 buckets gives 48 partitions on the
large side at about 208 rows each. That is small, and deliberately so: the point
is the query plan and the clustering behaviour, both of which are identical at
10,000 rows and at 5 billion.

If you raise `--employees`, raise `--buckets` with it. The renderer prints
rows-per-partition after every run and warns when partitions get too thin.

Do not use this data to make a timing claim. A 10,000-row table will not show the
full-refresh-versus-incremental gap in wall-clock terms, and asserting a speedup
from it is the kind of thing Kedia would catch. Demonstrate correctness and plan
shape here; make the performance argument from the pattern.

## What the generated data is built to demonstrate

**Deliberate skew.** About 30% of employees land in the first three departments,
so Adaptive Query Execution has genuine skew to coalesce and split at runtime.
Without skew, the AQE argument in the doc is theoretical.

**A many-to-one join that is genuinely many-to-one.** `department_name` is unique
in `gold_department`, verified by check 2. This is what lets AQE broadcast the
small side, which is the counter to hand-repartitioning both DataFrames.

**Clean referential integrity.** Every `department_name` on the large side resolves,
verified by check 3. The Gold+ row count must therefore equal `gold_employee`
exactly — check 10 is a one-line proof that the join neither fans out nor drops
rows.

**A 90-day watermark spread** on `updated_at`, so the incremental predicate has a
meaningful boundary rather than an all-or-nothing cut.

**Department transfers in the incremental batch.** `02_incremental_batch.sql` lands
500 new hires and moves 200 existing employees to a different department. The
transfers are the interesting half: an employee's `department_name` changes, so the
Gold+ row must be restated. Insert-only incremental logic gets this wrong and
`merge` on `employee_id` gets it right, which you can show rather than assert.

There is a second lesson hiding in those transfers. `department_number` is a
partition column, so the updates physically rewrite rows across partitions —
a concrete illustration of why partitioning on a mutable key hurts, and the
argument for Liquid Clustering on the Gold+ table. Worth saying out loud if
someone asks why the new table isn't partitioned the same way.

## The three model variants

`dbt/models/` carries all three options from the doc, so the room can pick:

* `gold_plus_employee_department` — `materialized='table'` with
  `liquid_clustered_by=['department_name']`. The baseline.
* `gold_plus_employee_department_incremental` — `merge` on `employee_id`, watermark
  filter inside the CTE so it pushes down. The realistic shape at his volume.
* `gold_plus_employee_department_auto` — `auto_liquid_cluster=true`, the
  defer-the-decision answer. Shipped with `enabled=false` so a `dbt build` does
  not create three near-identical tables; flip it on if the layout question comes up.

Tests are the argument, not decoration. `cost_center` is tested `not_null` on the
output: it comes from the small side, so a null means the join dropped an employee.
That is the failure a hand-written PySpark join fails silently on.

## Pre-flight

`sql/03_verify.sql` states the expected answer next to every check, so a wrong
result is obvious without cross-referencing. Two are worth running on screen:

Check 4, `DESCRIBE DETAIL`, shows `partitionColumns` on the sources and proves
`department_name` is absent from them. Check 8, `EXPLAIN FORMATTED`, shows a
`BroadcastHashJoin` on the small side and no partition pruning on the join key.
That plan output proves the reframe in the doc rather than asserting it, and it
is the most persuasive thing you can put in front of an engineer of his level.

Checks 9 and 10 are commented out because they reference the Gold+ table. Uncomment
after the first `dbt build`.

## Version floors

`auto_liquid_cluster` requires dbt-databricks 1.10.0+; per-model `skip_optimize`
requires 1.12.2+. Confirm the adapter version before the session, and note that
Fusion cannot currently build `metric_view` materializations on Databricks — not
relevant to this demo, but adjacent enough to be worth remembering if the
conversation drifts.
