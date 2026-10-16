import logging
from functools import reduce

from pyspark.sql import DataFrame  # type: ignore
from pyspark.sql import functions as f  # type: ignore

from data_engineering.core.sanity_check.base import BaseValidator, QualitySeverity
from data_engineering.core.sanity_check.utils import to_dataframe

logger = logging.getLogger(__name__)


class StandardDuplicatesValidator(BaseValidator):
    """Validates and detects duplicate records based on primary key configuration."""

    DEFAULT_FAIL_SEVERITY = QualitySeverity.HIGH

    def validate(self, p_df: DataFrame, p_dataset_name: str) -> DataFrame:
        """Validate for duplicate primary keys.

        The validation performs the following operations:
        1. Resolve primary keys from configuration (primary_key, pk, unique_id)
        2. Filter null values from key columns
        3. Count duplicates using groupBy aggregation
        4. Generate validation records with PASS/FAIL status

        Args:
            p_df: DataFrame to validate
            p_dataset_name: Name of dataset being validated

        Returns:
            DataFrame with validation results
        """
        primary_keys = self._get_config_value(
            ["primary_key", "pk", "unique_id", "unique_keys"], default=[]
        )

        if not primary_keys:
            return to_dataframe(self.spark, [])

        primary_keys = [primary_keys] if isinstance(primary_keys, str) else primary_keys

        actual_pks = []
        missing_keys = []
        for pk in primary_keys:
            actual_col = self._resolve_column(p_df, pk)
            if actual_col:
                actual_pks.append(actual_col)
            else:
                missing_keys.append(pk)

        if missing_keys:
            return to_dataframe(self.spark, [])

        records = []
        pk_conditions = [f.col(pk).isNotNull() for pk in actual_pks]
        pk_filter = reduce(lambda a, b: a & b, pk_conditions, f.lit(True))
        df_no_nulls = p_df.filter(pk_filter)
        dup_count = (
            df_no_nulls.groupBy(*actual_pks).count().filter(f.col("count") > 1).count()
        )
        pk_str = ", ".join(actual_pks)

        failed = dup_count > 0
        fail_desc = (
            f"Se detectaron {dup_count} registros duplicados "
            f"para la llave primaria: {pk_str}."
        )
        pass_desc = (
            f"No se encontraron registros duplicados "
            f"para la llave primaria: {pk_str}."
        )

        self._add_validation_record(
            records=records,
            dataset_name=p_dataset_name,
            check_name=pk_str,
            category="DUPLICATES",
            failed=failed,
            fail_description=fail_desc,
            pass_description=pass_desc,
            fail_severity=self._get_severity(self.DEFAULT_FAIL_SEVERITY),
        )

        return to_dataframe(self.spark, records)


class StandardNullValuesValidator(BaseValidator):
    """Validates null value percentages against configured thresholds."""

    DEFAULT_FAIL_SEVERITY = QualitySeverity.MEDIUM

    def validate(self, p_df: DataFrame, p_dataset_name: str) -> DataFrame:
        """Validate null value percentages with optimized single-pass aggregation.

        The validation performs the following operations:
        1. Resolve null thresholds from configuration.
        2. Calculate total row count and null counts per column in single aggregation
        3. Compare null percentages against configured thresholds
        4. Generate FAIL records for columns exceeding thresholds

        Args:
            p_df: DataFrame to validate
            p_dataset_name: Name of dataset being validated

        Returns:
            DataFrame with validation results per column
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

        if not merged_thresholds:
            return to_dataframe(self.spark, [])

        agg_exprs = []
        column_map = {}

        for col_name in merged_thresholds.keys():
            actual_col = self._resolve_column(p_df, col_name)

            if not actual_col:
                continue

            null_count_alias = f"null_count_{actual_col}"
            agg_exprs.append(
                f.count(f.when(f.col(actual_col).isNull(), 1)).alias(null_count_alias)
            )
            column_map[null_count_alias] = {
                "column": actual_col,
                "threshold": merged_thresholds[col_name],
            }

        if not agg_exprs:
            return to_dataframe(self.spark, [])

        agg_exprs.append(f.count(f.lit(1)).alias("__total_rows"))
        agg_result = p_df.agg(*agg_exprs).collect()[0]
        total_rows = agg_result["__total_rows"] or 1
        records = []

        for null_count_alias, col_info in column_map.items():
            actual_col = col_info["column"]
            max_null_percent = col_info["threshold"]

            null_count = agg_result[null_count_alias] or 0
            null_pct = (null_count / total_rows) * 100

            is_fail = (
                (null_count > 0)
                if max_null_percent == 0
                else (null_pct > max_null_percent)
            )

            fail_desc = (
                f"La columna '{actual_col}' tiene {null_pct:.2f}% valores "
                f"nulos ({null_count:,} registros), excediendo el umbral de "
                f"{max_null_percent}%."
            )
            pass_desc = (
                f"La columna '{actual_col}' tiene {null_pct:.2f}% valores "
                f"nulos, dentro del límite establecido de {max_null_percent}%."
            )

            self._add_validation_record(
                records=records,
                dataset_name=p_dataset_name,
                check_name=actual_col,
                category="NULL_VALUES",
                failed=is_fail,
                fail_description=fail_desc,
                pass_description=pass_desc,
                fail_severity=self._get_severity(self.DEFAULT_FAIL_SEVERITY),
            )

        logger.info(
            f"NULL_VALUES: Checked {len(records)} columns in {p_dataset_name} "
            f"with single-pass aggregation"
        )

        return to_dataframe(self.spark, records)


class StandardDimensionalValidator(BaseValidator):
    """Validates dimensional attributes against allowed values"""

    DEFAULT_FAIL_SEVERITY = QualitySeverity.MEDIUM

    def validate(self, p_df: DataFrame, p_dataset_name: str) -> DataFrame:
        """Validate dimensional attributes against allowed values/catalog.

        The validation performs the following operations:
        1. Load dimensional constraints and whitelist configurations
        2. Build aggregation expressions for all dimensions in single scan
        3. Count invalid (null or out-of-domain) values per dimension
        4. Generate FAIL records for dimensions with invalid values

        Args:
            p_df: DataFrame to validate
            p_dataset_name: Name of dataset being validated

        Returns:
            DataFrame with validation results per dimension
        """
        dimensions = self._get_config_value(
            ["dimensions", "allowed_values", "domain_constraints"], default={}
        )

        value_checks = self.config.get("value_checks", [])

        if not dimensions and not value_checks:
            return to_dataframe(self.spark, [])

        validator_allow_null = bool(self.config.get("allow_null", False))

        def _invalid_condition(p_col: str, p_allowed: list, p_allow_null: bool):
            """Build the out-of-domain condition, optionally treating NULL as valid."""
            out_of_domain = ~f.col(p_col).isin(p_allowed)
            if p_allow_null:
                return f.col(p_col).isNotNull() & out_of_domain
            return f.col(p_col).isNull() | out_of_domain

        records = []
        agg_exprs = []
        check_map = {}

        for col_name, allowed_vals in dimensions.items():
            actual_col = self._resolve_column(p_df, col_name)

            if not actual_col:
                logger.warning(
                    f"Dimension column '{col_name}' not found in "
                    f"dataset {p_dataset_name}"
                )
                continue

            if not allowed_vals:
                logger.warning(
                    f"No allowed values configured for column '{col_name}' "
                    f"in {p_dataset_name}"
                )
                continue

            alias = f"inv_{actual_col}"
            invalid_cond = _invalid_condition(
                actual_col, allowed_vals, validator_allow_null
            )
            agg_exprs.append(f.count(f.when(invalid_cond, 1)).alias(alias))
            check_map[alias] = {"column": actual_col, "type": "dimensional"}

        for i, check in enumerate(value_checks):
            col_name = check.get("column")
            valid_list = check.get("whitelist", [])

            if not col_name:
                continue

            actual_col = self._resolve_column(p_df, col_name)

            if not actual_col:
                continue

            if not valid_list:
                logger.warning(
                    f"No whitelist values configured for column '{col_name}' "
                    f"in {p_dataset_name}"
                )
                continue

            if actual_col.lower() in [k.lower() for k in dimensions.keys()]:
                continue

            check_allow_null = bool(check.get("allow_null", validator_allow_null))
            alias = f"vc_{i}_{actual_col}"
            invalid_cond = _invalid_condition(actual_col, valid_list, check_allow_null)
            agg_exprs.append(f.count(f.when(invalid_cond, 1)).alias(alias))
            check_map[alias] = {"column": actual_col, "type": "dimensional"}

        if not agg_exprs:
            return to_dataframe(self.spark, [])

        results = p_df.agg(*agg_exprs).collect()[0].asDict()

        for alias, count in results.items():
            info = check_map[alias]
            col_name = info["column"]

            failed = count > 0
            fail_desc = (
                f"La columna '{col_name}' tiene {count} registros con valores "
                f"fuera de la restricción dimensional configurada."
            )
            pass_desc = (
                f"La columna '{col_name}' tiene todos los valores "
                f"dentro de la restricción dimensional configurada."
            )

            self._add_validation_record(
                records=records,
                dataset_name=p_dataset_name,
                check_name=col_name,
                category="DIMENSIONAL",
                failed=failed,
                fail_description=fail_desc,
                pass_description=pass_desc,
                fail_severity=self._get_severity(self.DEFAULT_FAIL_SEVERITY),
            )

        return to_dataframe(self.spark, records)


class StandardConsistencyValidator(BaseValidator):
    """Validates dataset consistency based on SQL-like rules and constraints."""

    def validate(self, p_df: DataFrame, p_dataset_name: str) -> DataFrame:
        """Validate dataset consistency based on SQL-like rules.

        The validation performs the following operations:
        1. Load consistency rules from configuration (rules array)
        2. Build aggregation expressions for all rules (single-pass optimization)
        3. Execute batch aggregation and evaluate each rule
        4. Generate FAIL/PASS records with rule-specific severity levels

        Args:
            p_df: DataFrame to validate
            p_dataset_name: Name of dataset being validated

        Returns:
            DataFrame with validation results per rule
        """
        rules = self.config.get("rules", [])
        if not rules:
            return to_dataframe(self.spark, [])

        agg_exprs = []
        rule_metadata = []

        for idx, rule in enumerate(rules):
            rule_name = rule.get("name", f"rule_{idx}")
            condition = rule.get("condition")

            if not condition:
                logger.warning(
                    f"Rule '{rule_name}' has no condition in {p_dataset_name}"
                )
                continue

            severity = self.coerce_severity(
                rule.get("severity"), self._get_severity(QualitySeverity.MEDIUM)
            )

            treat_null_as_fail = bool(rule.get("treat_null_as_fail", False))

            alias = f"invalid_count_{idx}"
            try:
                violated = ~f.expr(condition)
                if treat_null_as_fail:
                    violated = f.coalesce(violated, f.lit(True))
                agg_exprs.append(f.count(f.when(violated, 1)).alias(alias))
                rule_metadata.append(
                    {
                        "idx": idx,
                        "alias": alias,
                        "name": rule_name,
                        "condition": condition,
                        "severity": severity,
                    }
                )
            except Exception as e:
                logger.error(f"Invalid rule condition '{rule_name}': {e}")
                continue

        if not agg_exprs:
            logger.warning(f"No valid consistency rules for {p_dataset_name}")
            return to_dataframe(self.spark, [])

        agg_exprs.append(f.count(f.lit(1)).alias("__total_rows"))
        agg_result = p_df.agg(*agg_exprs).collect()[0]
        total_rows = agg_result["__total_rows"] or 1

        records = []
        for meta in rule_metadata:
            invalid_count = agg_result[meta["alias"]]
            failed = invalid_count > 0

            fail_desc = (
                f"La regla de consistencia '{meta['name']}' falló para "
                f"{invalid_count} de {total_rows} registros ({meta['condition']})."
            )
            pass_desc = (
                f"La regla de consistencia '{meta['name']}' pasó para "
                f"todos los registros ({meta['condition']})."
            )

            self._add_validation_record(
                records=records,
                dataset_name=p_dataset_name,
                check_name=meta["name"],
                category="CONSISTENCY",
                failed=failed,
                fail_description=fail_desc,
                pass_description=pass_desc,
                fail_severity=meta["severity"],
            )

        return to_dataframe(self.spark, records)
