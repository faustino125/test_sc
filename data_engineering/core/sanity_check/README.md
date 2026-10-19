# Sanity Check and Quality Gate

This module enforces data quality controls before persistence and, in some execution paths, before upstream dependencies are consumed. It separates two responsibilities:

- **Sanity Check (SC):** Detects data quality issues, consistency violations, and business rule failures.
- **Quality Gate (QG):** Interprets SC results and decides whether execution can continue.

Sanity checks are enabled by default (`--sanity-check true`). Use `--sanity-check false` only when you intentionally want to bypass validation.

## What It Does

1. SC validates datasets against the rules defined in the layer configuration.
2. QG evaluates the validation results and decides whether to proceed.
3. Any **CRITICAL** failure blocks persistence or halts the dependent step.
4. Validation results are written to the quality history table for traceability.

## Standard Validators

The core exposes reusable validators that are activated through each layer's YAML configuration.

| Validator | Class | Config keys | Purpose |
| :--- | :--- | :--- | :--- |
| `duplicates` | `StandardDuplicatesValidator` | `primary_key` | Detects duplicate records using a simple or composite primary key. |
| `null_values` | `StandardNullValuesValidator` | `columns`, `null_thresholds` | Validates the allowed null percentage per column (`columns` alone means 0%). |
| `dimensional` | `StandardDimensionalValidator` | `dimensions`, `allow_null` | Verifies that values belong to the configured allowed set. |
| `consistency` | `StandardConsistencyValidator` | `rules` | Evaluates SQL-like business rules. |

Every validator also accepts `enabled` and `severity`. Any other key is reported in the logs as unknown and ignored, so a misspelled key is visible instead of silently disabling a check. An enabled type that is not in this table is also reported and skipped.

> **Consistency rules and NULLs:** By default a row whose rule condition evaluates to `NULL` (for example a `NULL` operand in `amount > 0`) is **not** counted as a failure, following standard SQL three-valued logic. Set `treat_null_as_fail: true` on an individual rule to count such rows as violations.

## Execution Model

The module follows a preventive model so that quality issues are caught as early as possible without adding unnecessary overhead.

### Core behavior

- **Upstream quality gate:** Before a node runs, the workflow may verify that its direct dependency has no persisted critical failures.
- **Pre-save quality gate:** The staged DataFrame is validated before persistence. When `overwriteSchema` is used, the DataFrame is still validated before the write call, so critical failures can block the save.
- **Quality history persistence:** Validation results are stored in the `sc_sanity_check_results` table for later inspection and gating.

### Layer timing

| Layer | Timing | Behavior |
| :--- | :--- | :--- |
| **Raw Data** | On-load (by Cleaning) | Raw sources are files landed by an external process; the pipeline never writes them. They are validated when a Cleaning node loads them: only the sources that node consumes and that have a config. Critical failures block that node. With `--sanity_check false` raw data is not validated. |
| **Cleaning** | On-load and pre-save | Validates its raw inputs first (see above), then validates the cleaned output before persistence. |
| **Transformation** | Pre-save | Enforces business rules before producing the transformed output. |
| **Integration** | Pre-save | Performs the final control before the data is exposed for consumption or BI. |

In general, each layer validates its own output and, in several cases, also validates the upstream input.

## Failure Handling

The quality gate **fails closed**: when in doubt, it blocks.

- If a validation reaches **CRITICAL** severity, the quality gate blocks persistence or marks the dependent step as failed.
- Non-critical failures are recorded, but they do not block execution by default.
- Validator execution errors are recorded and, by default, treated as blocking failures with **CRITICAL** severity, so the persisted history also blocks downstream layers. This behavior can be controlled with `block_on_validator_error` or the environment variable `sanity_check_block_on_validator_error` (when disabled, errors are recorded as HIGH).
- A dataset that cannot be loaded or validated (missing table, permissions, invalid metadata) is recorded as a CRITICAL `LOAD_ERROR` / `VALIDATION_ERROR` and blocks; it is never skipped as if it had no data.
- A column configured in a check but missing from the dataset is recorded as a `MISSING_COLUMN` failure with the severity of that check.
- An invalid consistency rule (SQL error, unknown column, non-boolean result) fails only that rule, with the rule's severity.
- Empty datasets/months are recorded as `EMPTY_DATASET` (WARN, or CRITICAL with `fail_on_empty: true`).
- When no check applies (validation disabled, no enabled validators), a `NO_CHECKS` record with status `SKIPPED` is written so older results for the same dataset and month are replaced.
- If the quality history table does not exist yet, the first run creates it (as an external table, see *Results table*) and execution continues. If the layer schema `<env>_<layer>` does not exist, the write fails with a clear error: the sanity check never creates schemas.
- Any other problem verifying the quality gate (invalid dates, permission or connection errors, missing schema/catalog) blocks execution.
- Nodes blocked by the quality gate do not stop the other nodes of the run, but once all nodes have run they are notified in Teams and the execution fails. Quality blocks are raised as `QualityGateError` (a `RuntimeError` subclass); any other error, including a sanity check that cannot run, stops the run as a system error.
- The historical quality gate counts a CRITICAL result when its month overlaps the requested range (`_observ_start_dt <= end_dt AND _observ_end_dt >= start_dt`), so a range ending mid-month still sees that month's results.

### Pre-save validation

- Every staged row is validated. Every month of `start_dt` → `end_dt` is reported (a month without rows as `EMPTY_DATASET`), plus any month the data has outside that range, so the results of those months are always replaced, never left over from an older run.
- Rows with a null date are validated as their own group (`SIN_FECHA`), stamped with the month of `end_dt`. Every result is keyed by the month end (`_observ_end_dt`), never by a mid-month date.
- The `_observ_start_dt` / `_observ_end_dt` columns that `save_data_flow` derives before writing are added for validation, so checks on them see the persisted values.
- When the dataset has a sanity check config, this fresh validation decides.
- A dataset without a config is persisted **without quality checks**: a warning is logged and a `NO_CHECKS` / `SKIPPED` record is written for each month of the run (`start_dt` → `end_dt`), so coverage gaps are visible in the results table (`WHERE validacion = 'NO_CHECKS'`) and older results for those months are replaced.


> Use `--sanity-check false` only when you intentionally want to bypass validation.

## Execution Model for Long Histories

Validation is **month-grouped**: all months of a run are evaluated in a few Spark jobs instead of filtering and validating month by month (implemented in `grouped.py`).

1. **Counts (one job for the whole range).** Null, dimensional and consistency checks are conditional counts, so they are computed together with `groupBy(month).agg(...)`. Memory is constant: each task keeps one set of counters per month, whatever the data volume.
2. **Duplicates (one shuffle per block).** Keys are counted per `(month, key)` in blocks of `batch_months` months. Each block is read with a date filter on the source column, so Delta only reads that block's partitions.
3. **Results (few writes).** Records are built in Python from the collected rows. A layer sanity check flow buffers the records of all its datasets and writes them once at the end (earlier if the buffer reaches `sanity_check_results_flush_records`); if a dataset fails, what was validated before it is still written. Pre-save validation writes once per block. Each write is one Spark job and one atomic Delta commit (see *Results table*).
4. **Raw sources read only the months validated.** Raw paths are `<table>/yyyy/mm/...`; a path ending in `/*/*/*` is read as `…/yyyy/mm/*` for the months of the range (`…/{yyyy/mm,…}/*` for several). If none of those folders exists, the whole path is read as before, so an empty range still ends as `EMPTY_DATASET`. The date filter on the column still applies on top.
5. **Nodes without checks do not start a flow.** When a node has no configured dataset to validate, no `sanity_check_flow` run is created.
4. **Few metadata requests.** With Spark Connect every `df.columns` / `df.schema` is a round trip to the server, so each dataset reads its schema once and reuses it for the date filter, column pruning and every validator.

Results keep the same granularity as before: one row per check and month, stamped with `_observ_start_dt` / `_observ_end_dt`.

| Setting | Where | Default | Effect |
| :--- | :--- | :--- | :--- |
| `batch_months` | dataset config (YAML) | `12` | Months per duplicates shuffle and per results write. Lower it if the cluster struggles with the shuffle of a very large dataset. |
| `sanity_check_batch_months` | environment variable | `12` | Same, for every dataset without its own `batch_months`. |
| `sanity_check_pre_save_cache` | environment variable | `disk` | Pre-save keeps only the checked columns and materializes them once with `DISK_ONLY`, so the transformation is not recomputed per pass. `none` recomputes instead (no local disk used). Where `persist` is not supported (e.g. serverless) it falls back to recomputing. |
| `sanity_check_results_flush_records` | environment variable | `20000` | Buffered result records that trigger an early write in a layer flow. |
| `sanity_check_raw_month_folders` | environment variable | `1` | Read only the `yyyy/mm` folders of the range for raw paths ending in `/*/*/*`. `0` reads every folder. |
| `sanity_check_string_date_pushdown` | environment variable | `1` | For a STRING date column, add a text range (`col >= start AND col < day after end`) that the Parquet/Delta reader can push down, next to the exact `to_date()` condition. Both agree for ISO values (`YYYY-MM-DD...`); set `0` if a source stores dates in another text format. |

```yaml
your_table:
  batch_months: 6
  duplicates:
    enabled: true
    primary_key: ["id_cliente"]
```

### Profiling a run

Every phase logs its duration with the prefix `[SC-TIMING]` (load, aggregate counts, duplicates per block, persistence per block), and each dataset ends with a `[SC-SUMMARY]` line: rows evaluated, months, blocks, months with STOP and elapsed time.

### Results table

`<catalog>.<env>_<layer>.sc_sanity_check_results`, the table the quality gate reads, is an **external** Delta table partitioned by `_observ_end_dt`. When missing, the sanity check creates it with `CREATE TABLE IF NOT EXISTS … USING DELTA LOCATION '<company_path>/<env>/<layer>/sanity_check/sc_sanity_check_results'` (`de/<env>/` in sandbox), never with a managed location. Existing tables keep their type; check it with `DESCRIBE TABLE EXTENDED` (`Type`, `Location`).

Each write replaces only the `(nombre_dataset, _observ_end_dt)` pairs being written, with `replaceWhere` in the same Delta commit as the new rows: a failed write never leaves those months without results, and `DESCRIBE HISTORY` shows one `WRITE` per write (no separate `DELETE`). Creating the table needs `CREATE EXTERNAL TABLE` on the external location and `CREATE TABLE` on the layer schema.

### Results table maintenance

The writes no longer run `VACUUM`. Schedule periodic maintenance instead, for example weekly:

```sql
OPTIMIZE <catalog>.<schema>.sc_sanity_check_results ZORDER BY (nombre_dataset);
VACUUM <catalog>.<schema>.sc_sanity_check_results RETAIN 168 HOURS;
```

`ZORDER BY (nombre_dataset)` keeps each dataset's rows in few files, so replacing one dataset does not rewrite the whole month. These table properties make the frequent small writes cheaper:

```sql
ALTER TABLE <catalog>.<schema>.sc_sanity_check_results SET TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true'
);
```

### Notifications

A dataset with blocked months sends a single Teams message listing those months, instead of one message per month.

## Extending the Framework

A new check type is a `BaseValidator` subclass in `standard_validators.py`, registered in `STANDARD_VALIDATORS` (`engine.py`). It declares:

- `GROUPED_MODE`: `AGGREGATE` (conditional counts, implement `prepare_aggregations` / `build_records`) or `KEYED` (counts per key, implement `prepare_keys` / `build_keyed_records`).
- `CONFIG_KEYS`: the config keys it reads.
- `config_columns()`: the columns it reads, so column pruning keeps them.

```yaml
your_table:
  range_check:
    enabled: true
    severity: "CRITICAL"
```

## Configuration Location

- Rules live in `<domain>_sanity_check.yml` files: `raw_data/src/sanity_check/conf/` for raw data, and each domain folder under `<layer>/src/` for the other layers.
- Orchestration is triggered from `data_engineering/core/main_flow.py`, `data_engineering/core/load_data_flow.py`, and `data_engineering/core/save_data_flow.py`.
