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

| Validator | Class | Purpose |
| :--- | :--- | :--- |
| `duplicates` | `StandardDuplicatesValidator` | Detects duplicate records using a simple or composite primary key. |
| `null_values` | `StandardNullValuesValidator` | Validates allowed null percentage or count per column. |
| `dimensional` | `StandardDimensionalValidator` | Verifies that values belong to the configured allowed set. |
| `consistency` | `StandardConsistencyValidator` | Evaluates SQL-like business rules. |

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
| **Raw Data** | Pre-save | Validates the source before persistence. Critical failures halt the load. |
| **Cleaning** | On-load and pre-save | Validates the upstream input first, then validates the cleaned output before persistence. |
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
- If the quality history table does not exist yet, the first run initializes it and execution continues.
- Any other problem verifying the quality gate (invalid dates, permission or connection errors, missing schema/catalog) blocks execution.
- Nodes blocked by the quality gate do not stop the other nodes of the run, but once all nodes have run they are notified in Teams and the execution fails.

### Pre-save validation

- Every staged row is validated: with a date column, the months come from the data itself (not from `start_dt`/`end_dt`), and rows with a null date are validated as their own group, stamped with `end_dt`.
- The `_observ_start_dt` / `_observ_end_dt` columns that `save_data_flow` derives before writing are added for validation, so checks on them see the persisted values.
- When the dataset has a sanity check config, this fresh validation decides; the historical quality gate is only consulted for datasets without a config.


> Use `--sanity-check false` only when you intentionally want to bypass validation.

## Execution Model for Long Histories

Validation is **month-grouped**: all months of a run are evaluated in a few Spark jobs instead of filtering and validating month by month (implemented in `grouped.py`).

1. **Counts (one job for the whole range).** Null, dimensional and consistency checks are conditional counts, so they are computed together with `groupBy(month).agg(...)`. Memory is constant: each task keeps one set of counters per month, whatever the data volume.
2. **Duplicates (one shuffle per block).** Keys are counted per `(month, key)` in blocks of `batch_months` months. Each block is read with a date filter on the source column, so Delta only reads that block's partitions.
3. **Results (one write per block).** Records are built in Python from the collected rows and persisted once per block, so a failure late in a long history keeps the earlier blocks.

Results keep the same granularity as before: one row per check and month, stamped with `_observ_start_dt` / `_observ_end_dt`. Layer-specific validators without a grouped mode still run month by month.

| Setting | Where | Default | Effect |
| :--- | :--- | :--- | :--- |
| `batch_months` | dataset config (YAML) | `12` | Months per duplicates shuffle and per results write. Lower it if the cluster struggles with the shuffle of a very large dataset. |
| `sanity_check_batch_months` | environment variable | `12` | Same, for every dataset without its own `batch_months`. |
| `sanity_check_pre_save_cache` | environment variable | `disk` | Pre-save keeps only the checked columns and materializes them once with `DISK_ONLY`, so the transformation is not recomputed per pass. `none` recomputes instead (no local disk used). |

```yaml
your_table:
  batch_months: 6
  duplicates:
    enabled: true
    primary_key: ["id_cliente"]
```

### Profiling a run

Every phase logs its duration with the prefix `[SC-TIMING]` (load, aggregate counts, duplicates per block, persistence per block), and each dataset ends with a `[SC-SUMMARY]` line: rows evaluated, months, blocks, months with STOP and elapsed time.

### Results table maintenance

Writes of `sc_sanity_check_results` replace only the `(nombre_dataset, _observ_end_dt)` pairs being written and no longer run `VACUUM` each time. Schedule periodic maintenance instead, for example weekly:

```sql
OPTIMIZE <catalog>.<schema>.sc_sanity_check_results;
VACUUM <catalog>.<schema>.sc_sanity_check_results RETAIN 168 HOURS;
```

### Notifications

A dataset with blocked months sends a single Teams message listing those months, instead of one message per month.

## Extending the Framework

Custom validators can be added without changing the core engine. Follow this pattern:

### 1. Define and register the validator

**File:** `data_engineering/{layer}/src/sanity_check/validators.py`

```python
class RangeValidator(BaseValidator):
    def validate(self, df: DataFrame, dataset_name: str) -> DataFrame:
        # Validation logic here
        return failed_df


class CleaningValidatorRegistry:
    VALIDATORS = {"range_check": RangeValidator}
```

### 2. Configure the validator in the layer YAML

**File:** `.../src/sanity_check/sanity_check_schema.yml`

```yaml
your_table:
  range_check:
    enabled: true
    severity: "CRITICAL"
```

## Layer Registries

The standard validators are available in every layer. A layer-specific registry adds its validators on top and can override a standard type by registering the same name.

> Enabling a layer-specific validator disables column pruning for that dataset, since the engine cannot know which columns it reads.

| Layer | Registry |
| :--- | :--- |
| **Raw Data** | `RawValidatorRegistry` |
| **Cleaning** | `CleaningValidatorRegistry` |
| **Transformation** | `TransformationValidatorRegistry` |
| **Integration** | `IntegrationDataValidatorRegistry` |

## Configuration Location

- Layer-specific rules live in `.../src/sanity_check/sanity_check_schema.yml`.
- Layer-specific validators can be registered in `data_engineering/{layer}/src/sanity_check/validators.py`.
- Orchestration is triggered from `data_engineering/core/main_flow.py`, `data_engineering/core/load_data_flow.py`, and `data_engineering/core/save_data_flow.py`.
