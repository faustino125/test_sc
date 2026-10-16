import logging
from typing import Any, Dict, List, Mapping, Tuple

from pyspark.sql import Column, DataFrame  # type: ignore
from pyspark.sql import functions as f  # type: ignore
from pyspark.sql.types import BooleanType  # type: ignore

from data_engineering.core.sanity_check.base import (
    BaseValidator,
    GroupedMode,
    QualitySeverity,
    ValidationRecord,
    ValidationStatus,
)
from data_engineering.core.sanity_check.utils import create_record

logger = logging.getLogger(__name__)


class StandardDuplicatesValidator(BaseValidator):
    """Validates and detects duplicate records based on primary key configuration.

    Duplicates are evaluated within each month: the engine groups by
    (month, key), so a key repeated in two different months is not a duplicate.
    """

    DEFAULT_FAIL_SEVERITY = QualitySeverity.HIGH
    GROUPED_MODE = GroupedMode.KEYED

    def prepare_keys(
        self, p_df: DataFrame, p_dataset_name: str
    ) -> Tuple[List[str], List[ValidationRecord]]:
        """Resolve primary keys from configuration (primary_key, pk, unique_id).

        Configured key columns missing from the DataFrame are reported as FAIL
        and disable the check, since duplicates cannot be computed without them.
        """
        primary_keys = self._get_config_value(
            ["primary_key", "pk", "unique_id", "unique_keys"], default=[]
        )
        if not primary_keys:
            return [], []

        primary_keys = [primary_keys] if isinstance(primary_keys, str) else primary_keys
        self._fail_severity = self._get_severity(self.DEFAULT_FAIL_SEVERITY)
        records: List[ValidationRecord] = []

        actual_pks = []
        for pk in primary_keys:
            actual_col = self._resolve_column(p_df, pk)
            if actual_col:
                actual_pks.append(actual_col)
            else:
                self._add_missing_column_record(
                    records, p_dataset_name, pk, "DUPLICATES", self._fail_severity
                )

        if records:
            return [], records

        self._pk_str = ", ".join(actual_pks)
        return actual_pks, []

    def build_keyed_records(
        self, p_row: Mapping[str, Any], p_dataset_name: str
    ) -> List[ValidationRecord]:
        """Build the DUPLICATES record of one month from its key statistics."""
        dup_keys = p_row.get("dup_keys") or 0
        dup_rows = p_row.get("dup_rows") or 0
        null_key_rows = p_row.get("null_key_rows") or 0

        null_note = (
            f" {null_key_rows:,} registros con llave nula no fueron evaluados."
            if null_key_rows
            else ""
        )
        records: List[ValidationRecord] = []
        self._add_validation_record(
            records=records,
            dataset_name=p_dataset_name,
            check_name=self._pk_str,
            category="DUPLICATES",
            failed=dup_keys > 0,
            fail_description=(
                f"Se detectaron {dup_keys:,} valores de llave duplicados "
                f"({dup_rows:,} registros afectados) para la llave primaria: "
                f"{self._pk_str}.{null_note}"
            ),
            pass_description=(
                f"No se encontraron registros duplicados "
                f"para la llave primaria: {self._pk_str}.{null_note}"
            ),
            fail_severity=self._fail_severity,
        )
        return records

    def validate(self, p_df: DataFrame, p_dataset_name: str) -> DataFrame:
        """Validate duplicate primary keys on the whole DataFrame."""
        return self._validate_grouped_once(p_df, p_dataset_name)


class StandardNullValuesValidator(BaseValidator):
    """Validates null value percentages against configured thresholds."""

    DEFAULT_FAIL_SEVERITY = QualitySeverity.MEDIUM
    GROUPED_MODE = GroupedMode.AGGREGATE

    @staticmethod
    def _coerce_threshold(p_column: str, p_value: Any) -> float:
        """Convert a configured null threshold into a percentage in [0, 100].

        Args:
            p_column: Column the threshold applies to (for error messages)
            p_value: Raw threshold from the YAML configuration

        Returns:
            Threshold as a float percentage.

        Raises:
            ValueError: If the threshold is not numeric or is out of range.
        """
        try:
            threshold = float(p_value)
        except (TypeError, ValueError):
            raise ValueError(
                f"Invalid null threshold for column '{p_column}': {p_value!r} "
                f"(expected a number between 0 and 100)."
            ) from None
        if not 0 <= threshold <= 100:
            raise ValueError(
                f"Invalid null threshold for column '{p_column}': {p_value!r} "
                f"(expected a number between 0 and 100)."
            )
        return threshold

    def prepare_aggregations(
        self, p_df: DataFrame, p_dataset_name: str, p_alias_prefix: str
    ) -> Tuple[List[Column], List[ValidationRecord]]:
        """Build one null count per configured column plus a row count.

        Thresholds are type-checked here; configured columns missing from the
        DataFrame are reported as FAIL.
        """
        null_thresholds = self._get_config_value(
            ["null_thresholds", "null_limits", "null_percentages"], default={}
        )

        base_columns = self._get_config_value(["columns", "fields"], default=[])
        if isinstance(base_columns, str):
            base_columns = [base_columns]

        merged_thresholds = {col: 0 for col in base_columns}
        if isinstance(null_thresholds, dict):
            merged_thresholds.update(null_thresholds)
        elif null_thresholds:
            logger.warning(
                f"Ignoring invalid null_thresholds for '{p_dataset_name}': "
                f"expected a mapping of column→percent, got "
                f"{type(null_thresholds).__name__}."
            )

        self._fail_severity = self._get_severity(self.DEFAULT_FAIL_SEVERITY)
        self._rows_alias = f"{p_alias_prefix}rows"
        self._checks: List[Tuple[str, str, float]] = []
        records: List[ValidationRecord] = []

        # Keyed by the resolved column so case variants of the same configured
        # column are evaluated once (the last configured threshold wins).
        thresholds_by_col: Dict[str, float] = {}
        for col_name, raw_threshold in merged_thresholds.items():
            threshold = self._coerce_threshold(col_name, raw_threshold)
            actual_col = self._resolve_column(p_df, col_name)
            if not actual_col:
                self._add_missing_column_record(
                    records,
                    p_dataset_name,
                    col_name,
                    "NULL_VALUES",
                    self._fail_severity,
                )
                continue
            thresholds_by_col[actual_col] = threshold

        if not thresholds_by_col:
            return [], records

        exprs = [f.count(f.lit(1)).alias(self._rows_alias)]
        for idx, (actual_col, threshold) in enumerate(thresholds_by_col.items()):
            alias = f"{p_alias_prefix}null_{idx}"
            exprs.append(f.count(f.when(f.col(actual_col).isNull(), 1)).alias(alias))
            self._checks.append((alias, actual_col, threshold))

        return exprs, records

    def build_records(
        self, p_row: Mapping[str, Any], p_dataset_name: str
    ) -> List[ValidationRecord]:
        """Compare null percentages against thresholds for one month."""
        total_rows = p_row.get(self._rows_alias) or 0
        records: List[ValidationRecord] = []

        for alias, actual_col, max_null_percent in self._checks:
            null_count = p_row.get(alias) or 0
            null_pct = (null_count / total_rows) * 100 if total_rows else 0.0

            is_fail = (
                (null_count > 0)
                if max_null_percent == 0
                else (null_pct > max_null_percent)
            )

            self._add_validation_record(
                records=records,
                dataset_name=p_dataset_name,
                check_name=actual_col,
                category="NULL_VALUES",
                failed=is_fail,
                fail_description=(
                    f"La columna '{actual_col}' tiene {null_pct:.2f}% valores "
                    f"nulos ({null_count:,} registros), excediendo el umbral de "
                    f"{max_null_percent:g}%."
                ),
                pass_description=(
                    f"La columna '{actual_col}' tiene {null_pct:.2f}% valores "
                    f"nulos, dentro del límite establecido de {max_null_percent:g}%."
                ),
                fail_severity=self._fail_severity,
            )
        return records

    def validate(self, p_df: DataFrame, p_dataset_name: str) -> DataFrame:
        """Validate null value percentages with a single-pass aggregation."""
        return self._validate_grouped_once(p_df, p_dataset_name)


class StandardDimensionalValidator(BaseValidator):
    """Validates dimensional attributes against allowed values"""

    DEFAULT_FAIL_SEVERITY = QualitySeverity.MEDIUM
    GROUPED_MODE = GroupedMode.AGGREGATE

    def prepare_aggregations(
        self, p_df: DataFrame, p_dataset_name: str, p_alias_prefix: str
    ) -> Tuple[List[Column], List[ValidationRecord]]:
        """Build one invalid-value count per dimension and value check.

        Configured columns missing from the DataFrame are reported as FAIL.
        """
        dimensions = self._get_config_value(
            ["dimensions", "allowed_values", "domain_constraints"], default={}
        )
        if not isinstance(dimensions, dict):
            raise ValueError(
                f"Invalid dimensional config for '{p_dataset_name}': expected a "
                f"mapping of column→allowed values, got {type(dimensions).__name__}."
            )

        value_checks = self.config.get("value_checks", []) or []
        validator_allow_null = bool(self.config.get("allow_null", False))
        self._fail_severity = self._get_severity(self.DEFAULT_FAIL_SEVERITY)
        self._checks: List[Tuple[str, str]] = []

        records: List[ValidationRecord] = []
        exprs: List[Column] = []
        dimension_cols = set()

        def _invalid_condition(p_col: str, p_allowed: list, p_allow_null: bool):
            """Build the out-of-domain condition, optionally treating NULL as valid."""
            out_of_domain = ~f.col(p_col).isin(p_allowed)
            if p_allow_null:
                return f.col(p_col).isNotNull() & out_of_domain
            return f.col(p_col).isNull() | out_of_domain

        def _add_check(p_col_name: str, p_allowed: Any, p_allow_null: bool) -> None:
            actual_col = self._resolve_column(p_df, p_col_name)
            if not actual_col:
                self._add_missing_column_record(
                    records,
                    p_dataset_name,
                    p_col_name,
                    "DIMENSIONAL",
                    self._fail_severity,
                )
                return
            if not p_allowed:
                logger.warning(
                    f"No allowed values configured for column '{p_col_name}' "
                    f"in {p_dataset_name}"
                )
                return
            if actual_col.lower() in dimension_cols:
                return
            dimension_cols.add(actual_col.lower())

            alias = f"{p_alias_prefix}invalid_{len(exprs)}"
            invalid_cond = _invalid_condition(actual_col, list(p_allowed), p_allow_null)
            exprs.append(f.count(f.when(invalid_cond, 1)).alias(alias))
            self._checks.append((alias, actual_col))

        for col_name, allowed_vals in dimensions.items():
            _add_check(col_name, allowed_vals, validator_allow_null)

        for check in value_checks:
            if not isinstance(check, dict) or not check.get("column"):
                continue
            _add_check(
                check["column"],
                check.get("whitelist", []),
                bool(check.get("allow_null", validator_allow_null)),
            )

        return exprs, records

    def build_records(
        self, p_row: Mapping[str, Any], p_dataset_name: str
    ) -> List[ValidationRecord]:
        """Build one PASS/FAIL record per dimension for one month."""
        records: List[ValidationRecord] = []
        for alias, col_name in self._checks:
            count = p_row.get(alias) or 0
            self._add_validation_record(
                records=records,
                dataset_name=p_dataset_name,
                check_name=col_name,
                category="DIMENSIONAL",
                failed=count > 0,
                fail_description=(
                    f"La columna '{col_name}' tiene {count:,} registros con valores "
                    f"fuera de la restricción dimensional configurada."
                ),
                pass_description=(
                    f"La columna '{col_name}' tiene todos los valores "
                    f"dentro de la restricción dimensional configurada."
                ),
                fail_severity=self._fail_severity,
            )
        return records

    def validate(self, p_df: DataFrame, p_dataset_name: str) -> DataFrame:
        """Validate dimensional attributes against allowed values/catalog."""
        return self._validate_grouped_once(p_df, p_dataset_name)


class StandardConsistencyValidator(BaseValidator):
    """Validates dataset consistency based on SQL-like rules and constraints."""

    GROUPED_MODE = GroupedMode.AGGREGATE

    def _invalid_rule_record(
        self,
        p_dataset_name: str,
        p_rule_name: str,
        p_severity: QualitySeverity,
        p_reason: str,
    ) -> ValidationRecord:
        """Build the FAIL record for a rule that cannot be evaluated."""
        return create_record(
            p_dataset_name=p_dataset_name,
            p_check_name=p_rule_name,
            p_category="CONSISTENCY",
            p_severity=p_severity,
            p_description=(
                f"La regla de consistencia '{p_rule_name}' no pudo evaluarse: "
                f"{p_reason[:500]}"
            ),
            p_status=ValidationStatus.FAIL,
        )

    def prepare_aggregations(
        self, p_df: DataFrame, p_dataset_name: str, p_alias_prefix: str
    ) -> Tuple[List[Column], List[ValidationRecord]]:
        """Analyze each rule on its own and build one violation count per rule.

        An invalid rule (bad severity, SQL error, unknown column, non-boolean
        result) becomes a FAIL record for that rule only.
        """
        rules = self.config.get("rules", []) or []
        default_severity = self._get_severity(QualitySeverity.MEDIUM)
        self._rows_alias = f"{p_alias_prefix}rows"
        self._rules: List[Dict[str, Any]] = []
        records: List[ValidationRecord] = []
        exprs: List[Column] = []

        for idx, rule in enumerate(rules):
            rule_name = rule.get("name", f"rule_{idx}")
            condition = rule.get("condition")

            if not condition:
                logger.warning(
                    f"Rule '{rule_name}' has no condition in {p_dataset_name}"
                )
                continue

            try:
                severity = self.coerce_severity(rule.get("severity"), default_severity)
            except ValueError as e:
                records.append(
                    self._invalid_rule_record(
                        p_dataset_name, rule_name, default_severity, str(e)
                    )
                )
                continue

            # Spark Connect builds expressions lazily, so force analysis per rule:
            # a parse error or unknown column must fail this rule, not all of them.
            try:
                rule_expr = f.expr(condition)
                data_type = p_df.select(rule_expr.alias("__rule")).schema[0].dataType
            except Exception as e:
                logger.error(f"Invalid rule condition '{rule_name}': {e}")
                records.append(
                    self._invalid_rule_record(
                        p_dataset_name, rule_name, severity, f"{condition} → {e}"
                    )
                )
                continue

            if not isinstance(data_type, BooleanType):
                records.append(
                    self._invalid_rule_record(
                        p_dataset_name,
                        rule_name,
                        severity,
                        f"{condition} devuelve {data_type.simpleString()}, "
                        f"se esperaba boolean.",
                    )
                )
                continue

            violated = ~rule_expr
            if bool(rule.get("treat_null_as_fail", False)):
                violated = f.coalesce(violated, f.lit(True))

            alias = f"{p_alias_prefix}invalid_{idx}"
            exprs.append(f.count(f.when(violated, 1)).alias(alias))
            self._rules.append(
                {
                    "alias": alias,
                    "name": rule_name,
                    "condition": condition,
                    "severity": severity,
                }
            )

        if not exprs:
            if not records and rules:
                logger.warning(f"No valid consistency rules for {p_dataset_name}")
            return [], records

        exprs.append(f.count(f.lit(1)).alias(self._rows_alias))
        return exprs, records

    def build_records(
        self, p_row: Mapping[str, Any], p_dataset_name: str
    ) -> List[ValidationRecord]:
        """Build one PASS/FAIL record per valid rule for one month."""
        total_rows = p_row.get(self._rows_alias) or 0
        records: List[ValidationRecord] = []

        for meta in self._rules:
            invalid_count = p_row.get(meta["alias"]) or 0
            self._add_validation_record(
                records=records,
                dataset_name=p_dataset_name,
                check_name=meta["name"],
                category="CONSISTENCY",
                failed=invalid_count > 0,
                fail_description=(
                    f"La regla de consistencia '{meta['name']}' falló para "
                    f"{invalid_count:,} de {total_rows:,} registros "
                    f"({meta['condition']})."
                ),
                pass_description=(
                    f"La regla de consistencia '{meta['name']}' pasó para "
                    f"todos los registros ({meta['condition']})."
                ),
                fail_severity=meta["severity"],
            )
        return records

    def validate(self, p_df: DataFrame, p_dataset_name: str) -> DataFrame:
        """Validate dataset consistency based on SQL-like rules."""
        return self._validate_grouped_once(p_df, p_dataset_name)
