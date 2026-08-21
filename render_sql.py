#!/usr/bin/env python3
"""
Render the Databricks SQL generator scripts for the Gold+ join-and-recluster demo.

Produces partitioned Delta tables shaped like Chetan Kedia's example:

    gold_employee    partitioned on (department_number, employee_id_bucket)   large side
    gold_department  partitioned on (department_number)                        small side
    join key         department_name  -- present in both, partitioned in neither

Everything is generated in place on Databricks with pure SQL. No files to upload,
no cluster libraries, no Spark needed locally.

Usage:
    python3 render_sql.py                                  # defaults: 10,000 employees
    python3 render_sql.py --employees 5000000 --buckets 32
    python3 render_sql.py --catalog jpmc_cib_demo --schema gold_demo

The catalog must already exist -- this renders CREATE SCHEMA but not
CREATE CATALOG. See the README.
"""

import argparse
import os

DEPARTMENTS = [
    "Payments Engineering", "Regulatory Reporting Technology", "Global Markets Technology",
    "Securities Services Platform", "Treasury Services Technology", "Trade Finance Engineering",
    "Liquidity Risk Technology", "Wholesale Payments Core", "Real Time Payments",
    "ACH and Wires Platform", "SWIFT Connectivity", "Client Onboarding Technology",
    "Reference Data Services", "Market Risk Analytics", "Credit Risk Technology",
    "Counterparty Risk Platform", "Collateral Management Tech", "Clearing and Settlement",
    "Custody Platform Engineering", "Fund Services Technology", "Middle Office Platform",
    "Post Trade Processing", "Equities Electronic Trading", "Fixed Income Trading Tech",
    "FX Trading Platform", "Commodities Technology", "Rates Technology",
    "Derivatives Processing", "Prime Brokerage Tech", "Research Platform Engineering",
    "Regulatory Change Delivery", "CCAR and Stress Testing", "Basel Reporting Platform",
    "MiFID Reporting Engineering", "EMIR and SFTR Reporting", "Transaction Reporting Ops Tech",
    "Financial Crime Technology", "Sanctions Screening Platform", "AML Analytics",
    "KYC Platform Engineering", "Fraud Detection Engineering", "Cyber Data Platform",
    "Identity and Access Engineering", "Infrastructure Observability", "Site Reliability Engineering",
    "Network Services Engineering", "Cloud Platform Services", "Container Platform Engineering",
    "Data Platform Engineering", "Data Governance Technology", "Metadata and Lineage Services",
    "Enterprise Data Warehouse", "Streaming Platform Engineering", "Machine Learning Platform",
    "Quantitative Research Tech", "Client Reporting Technology", "Digital Channels Engineering",
    "API Gateway Services", "Corporate Functions Technology", "Finance Systems Engineering",
]

FIRST_NAMES = [
    "Aarav", "Priya", "Chen", "Maria", "James", "Fatima", "Daniel", "Yuki", "Omar", "Sofia",
    "Liam", "Ananya", "Wei", "Isabella", "Noah", "Zara", "Ethan", "Mei", "Rahul", "Elena",
    "Marcus", "Nadia", "Hiroshi", "Camila", "Tobias", "Lakshmi", "Dmitri", "Amara", "Felix", "Ingrid",
]

LAST_NAMES = [
    "Sharma", "Okafor", "Zhang", "Rodriguez", "Whitfield", "Al-Rashid", "Kowalski", "Tanaka",
    "Haddad", "Moreau", "Sullivan", "Iyer", "Liu", "Rossi", "Andersen", "Khan", "Brennan",
    "Nakamura", "Kedia", "Papadopoulos", "Volkov", "Mensah", "Lindqvist", "Ferreira",
    "Castellanos", "Nguyen", "Berhane", "Fitzgerald", "Yamamoto", "Dubois",
]

# JPMC-style global site codes
LOCATIONS = [
    "NYC1", "JRC1", "CMH1", "PLN1", "CHI1", "TPA1", "WLM1",
    "LDN1", "BMH1", "GLA1", "DUB1",
    "MUM1", "HYD1", "BLR1", "SGP1", "HKG1", "TOK1", "SYD1",
    "BUE1", "SAO1",
]

SALARY_BANDS = ["B1", "B2", "B3", "B4", "B5", "B6", "B7"]


def sql_array(values):
    inner = ", ".join("'" + v.replace("'", "''") + "'" for v in values)
    return "array(" + inner + ")"


def wrapped_sql_array(values, per_line=4, indent=8):
    items = ["'" + v.replace("'", "''") + "'" for v in values]
    lines, pad = [], " " * indent
    for i in range(0, len(items), per_line):
        lines.append(pad + ", ".join(items[i:i + per_line]))
    return "array(\n" + ",\n".join(lines) + "\n" + " " * (indent - 4) + ")"


def render_create(args):
    depts = DEPARTMENTS[: args.departments]
    n_dept = len(depts)
    fq_emp = f"{args.catalog}.{args.schema}.gold_employee"
    fq_dept = f"{args.catalog}.{args.schema}.gold_department"

    return f"""-- =====================================================================
-- 01_create_and_generate.sql
--
-- Gold+ join-and-recluster demo -- source table generation
-- Rendered by render_sql.py. Do not hand-edit; re-render instead.
--
--   employees   : {args.employees:,}
--   departments : {n_dept}
--   buckets     : {args.buckets}
--   as-of       : {args.asof}
--
-- Shape being reproduced (from the reference pattern doc):
--
--   gold_employee    partitioned on (department_number, employee_id_bucket)
--   gold_department  partitioned on (department_number)
--   join key         department_name -- in both tables, partitioned in neither
--
-- The whole point of the demo is that the join key is NOT a partition column
-- on either side, so the ingest-optimized layout cannot be used for pruning
-- or a co-located join. Do not "fix" the partitioning below.
--
-- Runs in a Databricks notebook or the SQL editor. Unity Catalog assumed.
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS {args.catalog}.{args.schema};

-- ---------------------------------------------------------------------
-- Small side: gold_department
-- Partitioned on department_number, per the source spec. One row per
-- department, so department_name is unique -- this is what makes the join
-- many-to-one and lets Adaptive Query Execution broadcast this side.
-- ---------------------------------------------------------------------

CREATE OR REPLACE TABLE {fq_dept} (
    department_number  INT     COMMENT 'Ingest-optimized partition key',
    department_name    STRING  COMMENT 'Join key for the Gold+ model. Unique here.',
    department_head    STRING,
    cost_center        STRING,
    division           STRING
)
USING DELTA
PARTITIONED BY (department_number)
COMMENT 'Synthetic Gold-layer department reference table. Small side of a many-to-one join.';

INSERT INTO {fq_dept}
WITH names AS (
    SELECT
        posexplode({wrapped_sql_array(depts)}) AS (idx, department_name)
)
SELECT
    idx + 1                                                            AS department_number,
    department_name,
    element_at({sql_array(FIRST_NAMES)}, 1 + pmod(hash(department_name, 'head_fn'), {len(FIRST_NAMES)}))
        || ' '
        || element_at({sql_array(LAST_NAMES)}, 1 + pmod(hash(department_name, 'head_ln'), {len(LAST_NAMES)}))
                                                                       AS department_head,
    'CC-' || lpad(cast(400000 + pmod(hash(department_name, 'cc'), 99999) AS STRING), 6, '0')
                                                                       AS cost_center,
    element_at(
        array('Commercial & Investment Bank', 'Markets', 'Securities Services',
              'Payments', 'Corporate Functions'),
        1 + pmod(hash(department_name, 'div'), 5)
    )                                                                  AS division
FROM names;

-- ---------------------------------------------------------------------
-- Large side: gold_employee
--
-- employee_id_bucket stands in for raw employee_id as a partition column.
-- Partitioning literally on employee_id would create one partition per
-- employee, which is unusable at any scale. The bucket preserves the only
-- property the demo depends on: the large table is physically laid out on
-- key columns, and department_name is not one of them.
--
-- department_name is denormalized onto this table on purpose -- that is
-- what makes a join on department_name possible without department_number,
-- and it mirrors the source spec.
-- ---------------------------------------------------------------------

CREATE OR REPLACE TABLE {fq_emp} (
    employee_id         BIGINT    COMMENT 'Grain. Unique key for the incremental merge.',
    employee_name       STRING,
    department_number   INT       COMMENT 'Ingest-optimized partition key',
    department_name     STRING    COMMENT 'Join key for the Gold+ model. Not a partition key.',
    location_code       STRING,
    salary_band         STRING,
    updated_at          TIMESTAMP COMMENT 'Watermark for the incremental variant',
    employee_id_bucket  INT       COMMENT 'Ingest-optimized partition key, stands in for employee_id'
)
USING DELTA
PARTITIONED BY (department_number, employee_id_bucket)
COMMENT 'Synthetic Gold-layer employee table. Large side of a many-to-one join.';

INSERT INTO {fq_emp}
WITH ids AS (
    SELECT 100000 + id AS employee_id
    FROM range(0, {args.employees})
),
assigned AS (
    SELECT
        employee_id,
        -- Deliberate skew: ~30% of employees land in the first three
        -- departments, so AQE has real skew to coalesce and split at runtime.
        CASE
            WHEN pmod(hash(employee_id, 'skew'), 100) < 30
                THEN 1 + pmod(hash(employee_id, 'top'), 3)
            ELSE 1 + pmod(hash(employee_id, 'dept'), {n_dept})
        END AS department_number
    FROM ids
)
SELECT
    a.employee_id,
    element_at({sql_array(FIRST_NAMES)}, 1 + pmod(hash(a.employee_id, 'fn'), {len(FIRST_NAMES)}))
        || ' '
        || element_at({sql_array(LAST_NAMES)}, 1 + pmod(hash(a.employee_id, 'ln'), {len(LAST_NAMES)}))
                                                                       AS employee_name,
    a.department_number,
    d.department_name,
    element_at({sql_array(LOCATIONS)}, 1 + pmod(hash(a.employee_id, 'loc'), {len(LOCATIONS)}))
                                                                       AS location_code,
    element_at({sql_array(SALARY_BANDS)}, 1 + pmod(hash(a.employee_id, 'band'), {len(SALARY_BANDS)}))
                                                                       AS salary_band,
    timestampadd(
        SECOND,
        -pmod(hash(a.employee_id, 'ts'), {args.history_days} * 86400),
        TIMESTAMP '{args.asof}'
    )                                                                  AS updated_at,
    pmod(a.employee_id, {args.buckets})                                AS employee_id_bucket
FROM assigned a
JOIN {fq_dept} d
    ON a.department_number = d.department_number;

-- ---------------------------------------------------------------------
-- Statistics. AQE decides broadcast-versus-shuffle from observed stats,
-- so collect them before timing anything or the first run misleads you.
-- ---------------------------------------------------------------------

ANALYZE TABLE {fq_dept} COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE {fq_emp}  COMPUTE STATISTICS FOR ALL COLUMNS;
"""


def render_incremental(args):
    fq_emp = f"{args.catalog}.{args.schema}.gold_employee"
    fq_dept = f"{args.catalog}.{args.schema}.gold_department"
    new_rows = args.new_rows
    upd_rows = args.updated_rows
    start = 100000 + args.employees

    return f"""-- =====================================================================
-- 02_incremental_batch.sql
--
-- Lands a new batch on gold_employee so the incremental Gold+ model has
-- something to do. Run this BETWEEN two dbt runs.
--
--   {new_rows:,} brand-new employees   -> exercise the merge INSERT path
--   {upd_rows:,} existing employees updated -> exercise the merge UPDATE path
--
-- Both carry updated_at after the as-of anchor, so the model's watermark
-- predicate picks them up and reads nothing else out of the large table.
--
-- Re-runnable: each invocation shifts the new-employee id range forward by
-- reading the current max, so you can land several batches in a session.
-- =====================================================================

-- ---------------------------------------------------------------------
-- New hires
-- ---------------------------------------------------------------------

INSERT INTO {fq_emp}
WITH anchor AS (
    SELECT coalesce(max(employee_id), {start}) + 1 AS start_id FROM {fq_emp}
),
ids AS (
    SELECT a.start_id + r.id AS employee_id
    FROM anchor a
    CROSS JOIN range(0, {new_rows}) r
),
assigned AS (
    SELECT
        employee_id,
        1 + pmod(hash(employee_id, 'newdept'), (SELECT count(*) FROM {fq_dept})) AS department_number
    FROM ids
)
SELECT
    a.employee_id,
    element_at({sql_array(FIRST_NAMES)}, 1 + pmod(hash(a.employee_id, 'fn'), {len(FIRST_NAMES)}))
        || ' '
        || element_at({sql_array(LAST_NAMES)}, 1 + pmod(hash(a.employee_id, 'ln'), {len(LAST_NAMES)}))
                                                                       AS employee_name,
    a.department_number,
    d.department_name,
    element_at({sql_array(LOCATIONS)}, 1 + pmod(hash(a.employee_id, 'loc'), {len(LOCATIONS)}))
                                                                       AS location_code,
    element_at({sql_array(SALARY_BANDS)}, 1 + pmod(hash(a.employee_id, 'band'), {len(SALARY_BANDS)}))
                                                                       AS salary_band,
    timestampadd(MINUTE, pmod(hash(a.employee_id, 'newts'), 720), TIMESTAMP '{args.asof}')
                                                                       AS updated_at,
    pmod(a.employee_id, {args.buckets})                                AS employee_id_bucket
FROM assigned a
JOIN {fq_dept} d
    ON a.department_number = d.department_number;

-- ---------------------------------------------------------------------
-- Internal transfers: existing employees change department.
--
-- This is the interesting half. The employee's department_name changes,
-- which means the Gold+ row has to be restated -- exactly the case a
-- naive insert-only incremental gets wrong and a merge gets right.
-- ---------------------------------------------------------------------

MERGE INTO {fq_emp} AS t
USING (
    WITH movers AS (
        SELECT employee_id
        FROM {fq_emp}
        WHERE updated_at < TIMESTAMP '{args.asof}'
        ORDER BY pmod(hash(employee_id, 'move'), 1000000)
        LIMIT {upd_rows}
    )
    SELECT
        m.employee_id,
        d.department_number AS new_department_number,
        d.department_name   AS new_department_name,
        timestampadd(MINUTE, pmod(hash(m.employee_id, 'movets'), 720), TIMESTAMP '{args.asof}')
            AS new_updated_at
    FROM movers m
    JOIN {fq_dept} d
        ON d.department_number =
           1 + pmod(hash(m.employee_id, 'movedept'), (SELECT count(*) FROM {fq_dept}))
) AS s
ON t.employee_id = s.employee_id
WHEN MATCHED THEN UPDATE SET
    t.department_number = s.new_department_number,
    t.department_name   = s.new_department_name,
    t.updated_at        = s.new_updated_at;

-- Note: department_number is a partition column, so these updates physically
-- rewrite rows into different partitions. That is realistic, and it is also a
-- concrete illustration of why partitioning on a mutable key hurts -- worth
-- mentioning out loud if the room asks why Liquid Clustering is preferred.
"""


def render_verify(args):
    fq_emp = f"{args.catalog}.{args.schema}.gold_employee"
    fq_dept = f"{args.catalog}.{args.schema}.gold_department"
    goldplus = f"{args.catalog}.{args.schema}.gold_plus_employee_department"

    return f"""-- =====================================================================
-- 03_verify.sql
--
-- Run after generation, before the session. Every check below should pass
-- or the demo misleads. Read the comments -- each one states the expected
-- answer, so a wrong result is obvious without cross-referencing.
-- =====================================================================

-- 1. Row counts. Expect {args.employees:,} employees, {min(args.departments, len(DEPARTMENTS))} departments.
SELECT 'gold_employee' AS table_name, count(*) AS rows FROM {fq_emp}
UNION ALL
SELECT 'gold_department', count(*) FROM {fq_dept};

-- 2. The join key must be unique on the small side, or the join is not
--    many-to-one and the row count fans out. Expect zero rows.
SELECT department_name, count(*) AS n
FROM {fq_dept}
GROUP BY department_name
HAVING count(*) > 1;

-- 3. Referential integrity on the join key. Expect zero rows.
--    If this returns anything, the Gold+ join silently drops employees.
SELECT e.department_name, count(*) AS orphaned_employees
FROM {fq_emp} e
LEFT JOIN {fq_dept} d
    ON e.department_name = d.department_name
WHERE d.department_name IS NULL
GROUP BY e.department_name;

-- 4. Partition layout. Confirms the join key is NOT a partition column --
--    the premise of the entire pattern. Look at the partitionColumns field.
DESCRIBE DETAIL {fq_emp};
DESCRIBE DETAIL {fq_dept};

-- 5. Partition count and rows per partition on the large side.
--    Watch for over-partitioning: if rows-per-partition is in the tens,
--    say so in the room rather than letting someone notice it. That is the
--    small-file problem Hive-style partitioning causes, and it is the
--    argument for Liquid Clustering on the Gold+ table.
SELECT
    count(*)                                              AS partitions,
    sum(rows)                                             AS total_rows,
    cast(avg(rows) AS INT)                                AS avg_rows_per_partition,
    min(rows)                                             AS min_rows,
    max(rows)                                             AS max_rows
FROM (
    SELECT department_number, employee_id_bucket, count(*) AS rows
    FROM {fq_emp}
    GROUP BY department_number, employee_id_bucket
);

-- 6. Skew check. The first three departments should hold roughly 30% of
--    employees between them. Real skew is what makes AQE's runtime
--    coalescing and split behaviour visible rather than theoretical.
SELECT
    department_number,
    department_name,
    count(*)                                                        AS employees,
    round(100.0 * count(*) / (SELECT count(*) FROM {fq_emp}), 2)    AS pct_of_total
FROM {fq_emp}
GROUP BY department_number, department_name
ORDER BY employees DESC
LIMIT 10;

-- 7. Watermark distribution. Confirms updated_at spans {args.history_days} days
--    so the incremental predicate has a meaningful boundary to cut on.
SELECT
    min(updated_at) AS earliest,
    max(updated_at) AS latest,
    count(DISTINCT date(updated_at)) AS distinct_days
FROM {fq_emp};

-- 8. The join plan. Confirm a BroadcastHashJoin on the small side, and that
--    no partition pruning is claimed on department_name. This is the single
--    most useful thing to show on screen: it proves the reframe rather than
--    asserting it.
EXPLAIN FORMATTED
SELECT e.employee_id, e.department_name, d.cost_center, d.division
FROM {fq_emp} e
JOIN {fq_dept} d
    ON e.department_name = d.department_name;

-- 9. After the dbt model has been built: confirm the clustering keys landed.
--    Look for clusteringColumns = [department_name] and note that
--    partitionColumns is empty -- the Gold+ table is laid out for reads,
--    not for ingest.
-- DESCRIBE DETAIL {goldplus};

-- 10. After the dbt model has been built: row count must equal
--     gold_employee exactly. A many-to-one join must neither fan out
--     nor drop rows, and this is the one-line proof.
-- SELECT
--     (SELECT count(*) FROM {fq_emp})  AS employee_rows,
--     (SELECT count(*) FROM {goldplus}) AS gold_plus_rows;
"""


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--catalog", default="main")
    p.add_argument("--schema", default="gold_demo")
    p.add_argument("--employees", type=int, default=10000)
    p.add_argument("--departments", type=int, default=24,
                   help=f"max {len(DEPARTMENTS)}")
    p.add_argument("--buckets", type=int, default=2,
                   help="employee_id_bucket cardinality; raise with scale")
    p.add_argument("--history-days", type=int, default=90)
    p.add_argument("--asof", default="2026-08-14 00:00:00",
                   help="fixed anchor timestamp, keeps output reproducible")
    p.add_argument("--new-rows", type=int, default=500)
    p.add_argument("--updated-rows", type=int, default=200)
    p.add_argument("--outdir", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "sql"))
    args = p.parse_args()

    if args.departments > len(DEPARTMENTS):
        p.error(f"--departments cannot exceed {len(DEPARTMENTS)}")
    if args.buckets < 1:
        p.error("--buckets must be >= 1")

    os.makedirs(args.outdir, exist_ok=True)
    files = {
        "01_create_and_generate.sql": render_create(args),
        "02_incremental_batch.sql": render_incremental(args),
        "03_verify.sql": render_verify(args),
    }
    for name, body in files.items():
        path = os.path.join(args.outdir, name)
        with open(path, "w") as f:
            f.write(body)
        print(f"wrote {path}  ({len(body):,} bytes)")

    n_dept = min(args.departments, len(DEPARTMENTS))
    parts = n_dept * args.buckets
    print(f"\n{args.employees:,} employees x {n_dept} departments, "
          f"{parts:,} partitions on the large side "
          f"(~{args.employees // max(parts, 1):,} rows each)")
    if args.employees // max(parts, 1) < 50:
        print("NOTE: few rows per partition. Fine for a correctness demo; "
              "lower --buckets or raise --employees if you want realistic file sizes.")


if __name__ == "__main__":
    main()
