-- =====================================================================
-- 01_create_and_generate.sql
--
-- Gold+ join-and-recluster demo -- source table generation
-- Rendered by render_sql.py. Do not hand-edit; re-render instead.
--
--   employees   : 5,000,000
--   departments : 24
--   buckets     : 32
--   as-of       : 2026-08-14 00:00:00
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

CREATE SCHEMA IF NOT EXISTS main.gold_demo;

-- ---------------------------------------------------------------------
-- Small side: gold_department
-- Partitioned on department_number, per the source spec. One row per
-- department, so department_name is unique -- this is what makes the join
-- many-to-one and lets Adaptive Query Execution broadcast this side.
-- ---------------------------------------------------------------------

CREATE OR REPLACE TABLE main.gold_demo.gold_department (
    department_number  INT     COMMENT 'Ingest-optimized partition key',
    department_name    STRING  COMMENT 'Join key for the Gold+ model. Unique here.',
    department_head    STRING,
    cost_center        STRING,
    division           STRING
)
USING DELTA
PARTITIONED BY (department_number)
COMMENT 'Synthetic Gold-layer department reference table. Small side of a many-to-one join.';

INSERT INTO main.gold_demo.gold_department
WITH names AS (
    SELECT
        posexplode(array(
        'Payments Engineering', 'Regulatory Reporting Technology', 'Global Markets Technology', 'Securities Services Platform',
        'Treasury Services Technology', 'Trade Finance Engineering', 'Liquidity Risk Technology', 'Wholesale Payments Core',
        'Real Time Payments', 'ACH and Wires Platform', 'SWIFT Connectivity', 'Client Onboarding Technology',
        'Reference Data Services', 'Market Risk Analytics', 'Credit Risk Technology', 'Counterparty Risk Platform',
        'Collateral Management Tech', 'Clearing and Settlement', 'Custody Platform Engineering', 'Fund Services Technology',
        'Middle Office Platform', 'Post Trade Processing', 'Equities Electronic Trading', 'Fixed Income Trading Tech'
    )) AS (idx, department_name)
)
SELECT
    idx + 1                                                            AS department_number,
    department_name,
    element_at(array('Aarav', 'Priya', 'Chen', 'Maria', 'James', 'Fatima', 'Daniel', 'Yuki', 'Omar', 'Sofia', 'Liam', 'Ananya', 'Wei', 'Isabella', 'Noah', 'Zara', 'Ethan', 'Mei', 'Rahul', 'Elena', 'Marcus', 'Nadia', 'Hiroshi', 'Camila', 'Tobias', 'Lakshmi', 'Dmitri', 'Amara', 'Felix', 'Ingrid'), 1 + pmod(hash(department_name, 'head_fn'), 30))
        || ' '
        || element_at(array('Sharma', 'Okafor', 'Zhang', 'Rodriguez', 'Whitfield', 'Al-Rashid', 'Kowalski', 'Tanaka', 'Haddad', 'Moreau', 'Sullivan', 'Iyer', 'Liu', 'Rossi', 'Andersen', 'Khan', 'Brennan', 'Nakamura', 'Kedia', 'Papadopoulos', 'Volkov', 'Mensah', 'Lindqvist', 'Ferreira', 'Castellanos', 'Nguyen', 'Berhane', 'Fitzgerald', 'Yamamoto', 'Dubois'), 1 + pmod(hash(department_name, 'head_ln'), 30))
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

CREATE OR REPLACE TABLE main.gold_demo.gold_employee (
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

INSERT INTO main.gold_demo.gold_employee
WITH ids AS (
    SELECT 100000 + id AS employee_id
    FROM range(0, 5000000)
),
assigned AS (
    SELECT
        employee_id,
        -- Deliberate skew: ~30% of employees land in the first three
        -- departments, so AQE has real skew to coalesce and split at runtime.
        CASE
            WHEN pmod(hash(employee_id, 'skew'), 100) < 30
                THEN 1 + pmod(hash(employee_id, 'top'), 3)
            ELSE 1 + pmod(hash(employee_id, 'dept'), 24)
        END AS department_number
    FROM ids
)
SELECT
    a.employee_id,
    element_at(array('Aarav', 'Priya', 'Chen', 'Maria', 'James', 'Fatima', 'Daniel', 'Yuki', 'Omar', 'Sofia', 'Liam', 'Ananya', 'Wei', 'Isabella', 'Noah', 'Zara', 'Ethan', 'Mei', 'Rahul', 'Elena', 'Marcus', 'Nadia', 'Hiroshi', 'Camila', 'Tobias', 'Lakshmi', 'Dmitri', 'Amara', 'Felix', 'Ingrid'), 1 + pmod(hash(a.employee_id, 'fn'), 30))
        || ' '
        || element_at(array('Sharma', 'Okafor', 'Zhang', 'Rodriguez', 'Whitfield', 'Al-Rashid', 'Kowalski', 'Tanaka', 'Haddad', 'Moreau', 'Sullivan', 'Iyer', 'Liu', 'Rossi', 'Andersen', 'Khan', 'Brennan', 'Nakamura', 'Kedia', 'Papadopoulos', 'Volkov', 'Mensah', 'Lindqvist', 'Ferreira', 'Castellanos', 'Nguyen', 'Berhane', 'Fitzgerald', 'Yamamoto', 'Dubois'), 1 + pmod(hash(a.employee_id, 'ln'), 30))
                                                                       AS employee_name,
    a.department_number,
    d.department_name,
    element_at(array('NYC1', 'JRC1', 'CMH1', 'PLN1', 'CHI1', 'TPA1', 'WLM1', 'LDN1', 'BMH1', 'GLA1', 'DUB1', 'MUM1', 'HYD1', 'BLR1', 'SGP1', 'HKG1', 'TOK1', 'SYD1', 'BUE1', 'SAO1'), 1 + pmod(hash(a.employee_id, 'loc'), 20))
                                                                       AS location_code,
    element_at(array('B1', 'B2', 'B3', 'B4', 'B5', 'B6', 'B7'), 1 + pmod(hash(a.employee_id, 'band'), 7))
                                                                       AS salary_band,
    timestampadd(
        SECOND,
        -pmod(hash(a.employee_id, 'ts'), 90 * 86400),
        TIMESTAMP '2026-08-14 00:00:00'
    )                                                                  AS updated_at,
    pmod(a.employee_id, 32)                                AS employee_id_bucket
FROM assigned a
JOIN main.gold_demo.gold_department d
    ON a.department_number = d.department_number;

-- ---------------------------------------------------------------------
-- Statistics. AQE decides broadcast-versus-shuffle from observed stats,
-- so collect them before timing anything or the first run misleads you.
-- ---------------------------------------------------------------------

ANALYZE TABLE main.gold_demo.gold_department COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE main.gold_demo.gold_employee  COMPUTE STATISTICS FOR ALL COLUMNS;
