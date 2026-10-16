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

- If a validation reaches **CRITICAL** severity, the quality gate blocks persistence or marks the dependent step as failed.
- Non-critical failures are recorded, but they do not block execution by default.
- Validator execution errors are also recorded and, by default, treated as blocking failures. This behavior can be controlled with `block_on_validator_error` or the environment variable `sanity_check_block_on_validator_error`.
- If the quality history table does not exist yet, the first run initializes it and execution continues.
- If the quality gate cannot be verified for reasons other than a missing history table, the default behavior is to fail closed and block execution.


> Use `--sanity-check false` only when you intentionally want to bypass validation.

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

The engine looks for layer-specific registry classes first and falls back to the standard validators when no custom registry is present.

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
