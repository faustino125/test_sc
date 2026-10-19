import logging
import re
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
    create_record,
)

logger = logging.getLogger(__name__)


def _str_list(p_value: Any) -> List[str]:
    """A column name or list of names as a list of strings (others dropped)."""
    if isinstance(p_value, str):
        return [p_value]
    if isinstance(p_value, list):
        return [v for v in p_value if isinstance(v, str)]
    return []


def _mapping_keys(p_value: Any) -> List[str]:
    """String keys of a mapping (column → setting), or an empty list."""
    return _str_list(list(p_value)) if isinstance(p_value, dict) else []


class StandardDuplicatesValidator(BaseValidator):
    """Validates and detects duplicate records based on primary key configuration.
    """

    DEFAULT_FAIL_SEVERITY = QualitySeverity.HIGH
    GROUPED_MODE = GroupedMode.KEYED
    CONFIG_KEYS = frozenset({"primary_key"})

    @classmethod
    def config_columns(cls, p_config: Mapping[str, Any]) -> Tuple[List[str], List[str]]:
        return _str_list(p_config.get("primary_key")), []

    def prepare_keys(
        self, p_df: DataFrame, p_dataset_name: str
    ) -> Tuple[List[str], List[ValidationRecord]]:
        """Resolve the primary key columns.
        """
        primary_keys = self._cfg("primary_key", [])
        if not primary_keys:
            return [], []

        primary_keys = [primary_keys] if isinstance(primary_keys, str) else primary_keys
        self._fail_severity = self._get_severity(self.DEFAULT_FAIL_SEVERITY)

        actual_pks = [self._resolve_column(p_df, pk) for pk in primary_keys]
        records = [
            self._missing_column_record(
                p_dataset_name, pk, "DUPLICATES", self._fail_severity
            )
            for pk, actual in zip(primary_keys, actual_pks)
            if not actual
        ]
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
        return [
            self._result_record(
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
        ]


class StandardNullValuesValidator(BaseValidator):
    """Validates null value percentages against configured thresholds."""

    DEFAULT_FAIL_SEVERITY = QualitySeverity.MEDIUM
    GROUPED_MODE = GroupedMode.AGGREGATE
    CONFIG_KEYS = frozenset({"columns", "null_thresholds"})

    @classmethod
    def config_columns(cls, p_config: Mapping[str, Any]) -> Tuple[List[str], List[str]]:
        columns = _str_list(p_config.get("columns"))
        return columns + _mapping_keys(p_config.get("null_thresholds")), []

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
            threshold = float("nan")
        if not 0 <= threshold <= 100:  # also rejects NaN
            raise ValueError(
                f"Invalid null threshold for column '{p_column}': {p_value!r} "
                f"(expected a number between 0 and 100)."
            )
        return threshold

    def prepare_aggregations(
        self, p_df: DataFrame, p_dataset_name: str, p_alias_prefix: str
    ) -> Tuple[List[Column], List[ValidationRecord]]:
        """Build one null count per configured column plus a row count.
        """
        null_thresholds = self._cfg("null_thresholds", {})
        base_columns = self._cfg("columns", [])
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
        thresholds_by_col: Dict[str, float] = {}
        for col_name, raw_threshold in merged_thresholds.items():
            threshold = self._coerce_threshold(col_name, raw_threshold)
            actual_col = self._resolve_column(p_df, col_name)
            if actual_col:
                thresholds_by_col[actual_col] = threshold
            else:
                records.append(
                    self._missing_column_record(
                        p_dataset_name, col_name, "NULL_VALUES", self._fail_severity
                    )
                )

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
            records.append(
                self._result_record(
                    dataset_name=p_dataset_name,
                    check_name=actual_col,
                    category="NULL_VALUES",
                    # With a 0% threshold any null fails (null_pct > 0).
                    failed=null_pct > max_null_percent,
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
            )
        return records


class StandardDimensionalValidator(BaseValidator):
    """Validates dimensional attributes against allowed values"""

    DEFAULT_FAIL_SEVERITY = QualitySeverity.MEDIUM
    GROUPED_MODE = GroupedMode.AGGREGATE
    CONFIG_KEYS = frozenset({"dimensions", "allow_null"})

    @classmethod
    def config_columns(cls, p_config: Mapping[str, Any]) -> Tuple[List[str], List[str]]:
        return _mapping_keys(p_config.get("dimensions")), []

    def prepare_aggregations(
        self, p_df: DataFrame, p_dataset_name: str, p_alias_prefix: str
    ) -> Tuple[List[Column], List[ValidationRecord]]:
        """Build one invalid-value count per dimension.

        Configured columns missing from the DataFrame are reported as FAIL.
        """
        dimensions = self._cfg("dimensions", {})
        if not isinstance(dimensions, dict):
            raise ValueError(
                f"Invalid dimensional config for '{p_dataset_name}': expected a "
                f"mapping of column→allowed values, got {type(dimensions).__name__}."
            )

        allow_null = bool(self.config.get("allow_null", False))
        self._fail_severity = self._get_severity(self.DEFAULT_FAIL_SEVERITY)
        self._checks: List[Tuple[str, str]] = []
        records: List[ValidationRecord] = []
        exprs: List[Column] = []
        dimension_cols = set()

        for col_name, allowed in dimensions.items():
            actual_col = self._resolve_column(p_df, col_name)
            if not actual_col:
                records.append(
                    self._missing_column_record(
                        p_dataset_name, col_name, "DIMENSIONAL", self._fail_severity
                    )
                )
                continue
            if not allowed:
                logger.warning(
                    f"No allowed values configured for column '{col_name}' "
                    f"in {p_dataset_name}"
                )
                continue
            if actual_col.lower() in dimension_cols:
                continue
            dimension_cols.add(actual_col.lower())

            out_of_domain = ~f.col(actual_col).isin(list(allowed))
            invalid = (
                f.col(actual_col).isNotNull() & out_of_domain
                if allow_null
                else f.col(actual_col).isNull() | out_of_domain
            )
            alias = f"{p_alias_prefix}invalid_{len(exprs)}"
            exprs.append(f.count(f.when(invalid, 1)).alias(alias))
            self._checks.append((alias, actual_col))

        return exprs, records

    def build_records(
        self, p_row: Mapping[str, Any], p_dataset_name: str
    ) -> List[ValidationRecord]:
        """Build one PASS/FAIL record per dimension for one month."""
        records: List[ValidationRecord] = []
        for alias, col_name in self._checks:
            count = p_row.get(alias) or 0
            records.append(
                self._result_record(
                    dataset_name=p_dataset_name,
                    check_name=col_name,
                    category="DIMENSIONAL",
                    failed=count > 0,
                    fail_description=(
                        f"La columna '{col_name}' tiene {count:,} registros con "
                        f"valores fuera de la restricción dimensional configurada."
                    ),
                    pass_description=(
                        f"La columna '{col_name}' tiene todos los valores "
                        f"dentro de la restricción dimensional configurada."
                    ),
                    fail_severity=self._fail_severity,
                )
            )
        return records


_SQL_TOKEN_SKIPLIST = frozenset(
    "and or not in is null true false between like rlike case when then else end "
    "as on exists distinct cast coalesce nullif asc desc interval current_date "
    "current_timestamp date timestamp ilike regexp div".split()
)
_SQL_STRING_LITERAL = re.compile(r"'(?:[^'\\]|\\.)*'|\"(?:[^\"\\]|\\.)*\"")
_SQL_IDENTIFIER = re.compile(
    r"`([^`]+)`|(?<![A-Za-z0-9_.])([A-Za-z_][A-Za-z0-9_.]*)\b(?!\s*\()"
)


def rule_identifiers(p_condition: str) -> List[str]:
    """Lower-cased identifiers a SQL condition references (columns, maybe).

    String literals, function calls and SQL keywords are ignored.
    """
    tokens = []
    without_literals = _SQL_STRING_LITERAL.sub(" ", p_condition)
    for quoted, plain in _SQL_IDENTIFIER.findall(without_literals):
        token = (quoted or plain).strip().rstrip(".").lower()
        if token and token not in _SQL_TOKEN_SKIPLIST:
            tokens.append(token)
    return tokens


class StandardConsistencyValidator(BaseValidator):
    """Validates dataset consistency based on SQL-like rules and constraints."""

    GROUPED_MODE = GroupedMode.AGGREGATE
    CONFIG_KEYS = frozenset({"rules"})

    @classmethod
    def config_columns(cls, p_config: Mapping[str, Any]) -> Tuple[List[str], List[str]]:
        rules = p_config.get("rules")
        conditions = [
            rule["condition"]
            for rule in (rules if isinstance(rules, list) else [])
            if isinstance(rule, dict) and isinstance(rule.get("condition"), str)
        ]
        return [], [token for c in conditions for token in rule_identifiers(c)]

    @staticmethod
    def _invalid_rule_record(
        p_dataset_name: str,
        p_rule_name: str,
        p_severity: QualitySeverity,
        p_reason: str,
    ) -> ValidationRecord:
        """Build the FAIL record for a rule that cannot be evaluated."""
        return create_record(
            p_dataset_name,
            p_rule_name,
            "CONSISTENCY",
            p_severity,
            f"La regla de consistencia '{p_rule_name}' no pudo evaluarse: "
            f"{p_reason[:500]}",
            ValidationStatus.FAIL,
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
        return [
            self._result_record(
                dataset_name=p_dataset_name,
                check_name=meta["name"],
                category="CONSISTENCY",
                failed=(p_row.get(meta["alias"]) or 0) > 0,
                fail_description=(
                    f"La regla de consistencia '{meta['name']}' falló para "
                    f"{p_row.get(meta['alias']) or 0:,} de {total_rows:,} registros "
                    f"({meta['condition']})."
                ),
                pass_description=(
                    f"La regla de consistencia '{meta['name']}' pasó para "
                    f"todos los registros ({meta['condition']})."
                ),
                fail_severity=meta["severity"],
            )
            for meta in self._rules
        ]
