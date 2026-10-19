"""Month-grouped sanity check execution for long histories.

Instead of filtering and validating one month at a time, every month is
evaluated in a few Spark jobs:

1. One `groupBy(month).agg(...)` with the conditional counts of all AGGREGATE
   validators (nulls, dimensions, consistency rules) plus rows per month.
   Memory is constant: each task only keeps one set of counters per month.
2. One `keyed_stats_frame` shuffle per block of `batch_months` for KEYED
   validators (duplicates). Blocks bound the shuffle size and are read with a
   date filter on the source column so Delta can prune partitions.

Results are built in Python from the collected rows and handed to a sink once
per block, so a failure late in a long history keeps the earlier blocks.
The records are identical to the per-month path: one row per check and month.
"""
import logging
import os
import time
from dataclasses import replace
from datetime import datetime
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
)

from pyspark.sql import Column, DataFrame  # type: ignore
from pyspark.sql import functions as f  # type: ignore

from data_engineering.core.sanity_check.base import (
    BaseValidator,
    GroupedMode,
    QualitySeverity,
    ValidationRecord,
    ValidationStatus,
    create_record,
    find_column,
    keyed_stats_frame,
)
from data_engineering.core.sanity_check.engine import LayerUtils, build_validators
from data_engineering.core.sanity_check.utils import _TIMEZONE_UTC_6, timed_phase

logger = logging.getLogger(__name__)

MONTH_COL = "__sc_month"
ROWS_ALIAS = "__sc_rows"
NULL_MONTH_LABEL = "SIN_FECHA"
DEFAULT_BATCH_MONTHS = 12
ENV_BATCH_MONTHS = "sanity_check_batch_months"
# Above a few hundred aggregate expressions Spark exceeds the 64KB generated
# method limit and falls back to slower interpreted code, so they are split.
MAX_AGG_EXPRS_PER_JOB = 200
ERROR_CATEGORIES = frozenset({"VALIDATOR_ERROR", "LOAD_ERROR", "VALIDATION_ERROR"})
# Key for keyed results that apply to every month of a block (job errors).
_ALL_MONTHS = "__all_months__"

MonthRange = Tuple[str, str]
RecordsSink = Callable[[List[ValidationRecord]], None]


# --------------------------------------------------------------------------- #
# Pure helpers
# --------------------------------------------------------------------------- #


def resolve_batch_months(p_config: Optional[Mapping[str, Any]]) -> int:
    """Months per block: dataset config `batch_months`, env var, or default.

    Args:
        p_config: Dataset validator configuration

    Returns:
        A positive number of months.
    """
    raw = (p_config or {}).get("batch_months")
    if raw is None:
        raw = os.getenv(ENV_BATCH_MONTHS)
    if raw is None or raw == "":
        return DEFAULT_BATCH_MONTHS
    try:
        value = int(raw)
    except (TypeError, ValueError):
        value = 0
    if value < 1:
        logger.warning(
            f"Invalid batch_months {raw!r}; using {DEFAULT_BATCH_MONTHS} months."
        )
        return DEFAULT_BATCH_MONTHS
    return value


def split_into_blocks(
    p_months: Sequence[MonthRange], p_batch_months: int
) -> List[List[MonthRange]]:
    """Split consecutive months into blocks of at most p_batch_months."""
    size = max(1, p_batch_months)
    return [list(p_months[i : i + size]) for i in range(0, len(p_months), size)]


def label_month_range(p_label_date: str) -> MonthRange:
    """Observation range used when rows cannot be attributed to a month.

    Args:
        p_label_date: Date stamped as _observ_end_dt (YYYY-MM-DD)

    Returns:
        (first day of that month, p_label_date)
    """
    return f"{p_label_date[:7]}-01", p_label_date


def default_label_date() -> str:
    """end_dt from the environment, or today (UTC-6)."""
    return os.getenv("end_dt") or datetime.now(_TIMEZONE_UTC_6).strftime("%Y-%m-%d")


def month_key(p_value: Any) -> Optional[str]:
    """Normalize a month value collected from Spark to 'YYYY-MM-DD'."""
    if p_value is None:
        return None
    if hasattr(p_value, "strftime"):
        return p_value.strftime("%Y-%m-%d")
    return str(p_value)[:10]


def rows_by_month(p_rows: Iterable[Any]) -> Iterator[Tuple[Optional[str], Dict]]:
    """(month key, values without the month column) of each collected row."""
    for row in p_rows:
        values = row.asDict()
        yield month_key(values.pop(MONTH_COL)), values


def as_bool(p_value: Any, p_default: bool) -> bool:
    """Interpret a boolean-like config/env value ("1", "yes", "off", 0, ...)."""
    if isinstance(p_value, bool):
        return p_value
    if isinstance(p_value, (int, float)):
        return p_value != 0
    if isinstance(p_value, str):
        normalized = p_value.strip().lower()
        if normalized in {"1", "true", "yes", "y", "on"}:
            return True
        if normalized in {"0", "false", "no", "n", "off"}:
            return False
    return p_default


def stamp_records(
    p_records: Sequence[ValidationRecord], p_start: str, p_end: str
) -> List[ValidationRecord]:
    """Copy records with their observation month set."""
    return [
        replace(r, _observ_start_dt=p_start, _observ_end_dt=p_end) for r in p_records
    ]


def validator_error_record(
    p_dataset_name: str,
    p_validator_name: str,
    p_error: Exception,
    p_severity: QualitySeverity,
) -> ValidationRecord:
    """Record for a validator that raised (same format as the per-month engine)."""
    return create_record(
        p_dataset_name=p_dataset_name,
        p_check_name=p_validator_name,
        p_category="VALIDATOR_ERROR",
        p_severity=p_severity,
        p_description=f"Validator execution error: {str(p_error)[:500]}",
        p_status=ValidationStatus.FAIL,
    )


def empty_dataset_record(
    p_dataset_name: str, p_fail_on_empty: bool
) -> ValidationRecord:
    """Record for a month without rows (CRITICAL FAIL with fail_on_empty)."""
    return create_record(
        p_dataset_name=p_dataset_name,
        p_check_name="__dataset__",
        p_category="EMPTY_DATASET",
        p_severity=QualitySeverity.CRITICAL if p_fail_on_empty else QualitySeverity.LOW,
        p_description=(
            f"El dataset '{p_dataset_name}' está vacío para el rango solicitado."
        ),
        p_status=ValidationStatus.FAIL if p_fail_on_empty else ValidationStatus.WARN,
    )


def skipped_record(p_dataset_name: str, p_reason: str) -> ValidationRecord:
    """Record for a month where no check was evaluated.

    It is still written so MERGE_BY_DATASET replaces older results for the
    month (a stale CRITICAL would otherwise keep blocking the quality gate).
    """
    return create_record(
        p_dataset_name=p_dataset_name,
        p_check_name="__dataset__",
        p_category="NO_CHECKS",
        p_severity=QualitySeverity.LOW,
        p_description=p_reason,
        p_status=ValidationStatus.SKIPPED,
    )


def dataset_error_record(
    p_dataset_name: str, p_category: str, p_error: Exception
) -> ValidationRecord:
    """CRITICAL record for a dataset that could not be loaded or validated."""
    return create_record(
        p_dataset_name=p_dataset_name,
        p_check_name="__dataset__",
        p_category=p_category,
        p_severity=QualitySeverity.CRITICAL,
        p_description=(
            f"No fue posible validar el dataset: "
            f"{type(p_error).__name__}: {str(p_error)[:500]}"
        ),
        p_status=ValidationStatus.FAIL,
    )


def assemble_month_records(
    p_dataset_name: str,
    p_row: Optional[Mapping[str, Any]],
    p_static_records: Sequence[ValidationRecord],
    p_aggregate_validators: Sequence[BaseValidator],
    p_extra_records: Sequence[ValidationRecord],
    p_fail_on_empty: bool,
) -> List[ValidationRecord]:
    """Build the records of one month.

    An empty month only gets its EMPTY_DATASET record.

    Args:
        p_dataset_name: Dataset name
        p_row: Aggregate row of the month (None or 0 rows = empty month)
        p_static_records: Data-independent records (missing columns, bad rules)
        p_aggregate_validators: Validators whose build_records reads p_row
        p_extra_records: Keyed and per-month validator records of the month
        p_fail_on_empty: Whether an empty month is a CRITICAL failure

    Returns:
        Records of the month (not yet stamped with the observation dates).
    """
    if not p_row or not p_row.get(ROWS_ALIAS):
        return [empty_dataset_record(p_dataset_name, p_fail_on_empty)]

    records = [replace(r) for r in p_static_records]
    for validator in p_aggregate_validators:
        records.extend(validator.build_records(p_row, p_dataset_name))
    records.extend(p_extra_records)
    if not records:
        records.append(
            skipped_record(
                p_dataset_name,
                "Las validaciones habilitadas no generaron resultados "
                "(revisar su configuración).",
            )
        )
    return records


def summarize_month(
    p_dataset_name: str,
    p_month_label: str,
    p_records: Sequence[ValidationRecord],
    p_total_checks: int,
    p_block_on_validator_error: bool,
) -> Dict[str, Any]:
    """Summary and STOP/PASS decision of one month.

    STOP when there is a CRITICAL failure, or a validator/load error and
    block_on_validator_error is on.
    """
    failures = [r for r in p_records if r.estado == ValidationStatus.FAIL.value]
    critical = [r for r in failures if r.severidad == QualitySeverity.CRITICAL.value]
    errors = [r for r in p_records if r.validacion in ERROR_CATEGORIES]

    stop = bool(critical) or (p_block_on_validator_error and bool(errors))
    summary: Dict[str, Any] = {
        "dataset_name": p_dataset_name,
        "month": p_month_label,
        "total_checks": p_total_checks,
        "failures": len(failures),
        "critical_failures": len(critical),
        "validator_errors": len(errors),
        "block_on_validator_error": p_block_on_validator_error,
        "decision": "STOP" if stop else "PASS",
    }
    if errors:
        summary["error"] = errors[0].descripcion[:300]
    return summary


class TrackingSink:
    """Wraps a results sink to know what was persisted and whether it failed.

    A failure while persisting must propagate as is; a failure while
    validating is turned into error records for the months not yet written.
    """

    def __init__(self, p_sink: RecordsSink) -> None:
        self._sink = p_sink
        self.failed = False
        self.calls = 0
        self.written_months: set = set()

    def __call__(self, p_records: List[ValidationRecord]) -> None:
        try:
            self._sink(p_records)
        except Exception:
            self.failed = True
            raise
        self.calls += 1
        self.written_months.update(r._observ_end_dt for r in p_records)


def emit_records_per_month(
    p_dataset_name: str,
    p_template_records: Sequence[ValidationRecord],
    p_months: Sequence[MonthRange],
    p_batch_months: int,
    p_sink: RecordsSink,
) -> List[Dict[str, Any]]:
    """Persist the same records for every month (errors, skipped datasets).

    Returns:
        One summary per month.
    """
    summaries = []
    for block in split_into_blocks(p_months, p_batch_months):
        block_records: List[ValidationRecord] = []
        for m_start, m_end in block:
            records = stamp_records(p_template_records, m_start, m_end)
            summaries.append(summarize_month(p_dataset_name, m_end, records, 0, True))
            block_records.extend(records)
        p_sink(block_records)
    return summaries


def record_failure(
    p_error: Exception,
    p_dataset_name: str,
    p_category: str,
    p_months: Sequence[MonthRange],
    p_batch_months: int,
    p_sink: TrackingSink,
) -> List[Dict[str, Any]]:
    """Turn a load/validation error into CRITICAL records for unwritten months.

    Call it from an `except` block. An error raised by the sink itself is
    re-raised: the results could not be persisted, so they cannot be replaced.

    Returns:
        One STOP summary per month recorded.
    """
    if p_sink.failed:
        raise p_error
    logger.error(f"❌ {p_dataset_name}: {p_error}", exc_info=True)
    pending = [m for m in p_months if m[1] not in p_sink.written_months]
    return emit_records_per_month(
        p_dataset_name,
        [dataset_error_record(p_dataset_name, p_category, p_error)],
        pending,
        p_batch_months,
        p_sink,
    )


def notify_stops(
    p_dataset_name: str, p_summaries: Sequence[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """STOP summaries of a dataset, after sending their Teams notification."""
    stops = [s for s in p_summaries if s.get("decision") == "STOP"]
    LayerUtils._send_stop_notification(p_dataset_name, stops)
    return stops


def select_checked_columns(
    p_df: DataFrame,
    p_dataset_name: str,
    p_config: Dict[str, Any],
    p_date_col: Optional[str],
    p_columns: Optional[Sequence[str]] = None,
) -> Tuple[DataFrame, List[str]]:
    """Keep only the columns the enabled checks read (all if unsure).

    Args:
        p_columns: p_df's columns if already known (saves a schema request)

    Returns:
        Tuple of (DataFrame, its columns), so callers need not ask Spark again.
    """
    all_columns = list(p_columns) if p_columns is not None else list(p_df.columns)
    columns = LayerUtils._extract_columns_for_caching(p_config, p_date_col, all_columns)
    if len(columns) >= len(all_columns):
        return p_df, all_columns
    logger.info(
        f"'{p_dataset_name}': validating {len(columns)}/{len(all_columns)} "
        f"required columns."
    )
    return p_df.select(*columns), list(columns)


# --------------------------------------------------------------------------- #
# Spark execution
# --------------------------------------------------------------------------- #


class GroupedValidationRunner:
    """Validate a DataFrame for many months with a bounded number of jobs."""

    def run(
        self,
        p_df: DataFrame,
        p_dataset_name: str,
        p_config: Dict[str, Any],
        p_date_col: Optional[str],
        p_month_ranges: Optional[Sequence[MonthRange]],
        p_sink: RecordsSink,
        p_null_month_label: Optional[str] = None,
        p_columns: Optional[Sequence[str]] = None,
    ) -> List[Dict[str, Any]]:
        """Validate every month of p_df and persist the records block by block.

        Args:
            p_df: Data to validate (already restricted to the requested range)
            p_dataset_name: Dataset name for the records
            p_config: Dataset validator configuration
            p_date_col: Date column used to attribute rows to months. If it is
                None or missing, the whole DataFrame is one group whose result
                applies to every month in p_month_ranges.
            p_month_ranges: Months to report. None derives them from the data
                (min..max month, gaps reported as empty).
            p_sink: Called once per block with that block's records
            p_null_month_label: _observ_end_dt for rows without a month
                (null date, or no date column without p_month_ranges)
            p_columns: p_df's columns if already known (each `df.columns` is
                a round trip to the Spark Connect server)

        Returns:
            One summary per reported month (decision PASS/STOP).

        Raises:
            Exception: If the data cannot be read at all, or the sink fails.
        """
        started = time.perf_counter()
        ds = p_dataset_name
        null_label = p_null_month_label or default_label_date()
        batch_months = resolve_batch_months(p_config)
        fail_on_empty = as_bool(p_config.get("fail_on_empty"), False)
        block_on_error = as_bool(
            p_config.get(
                "block_on_validator_error",
                os.getenv("sanity_check_block_on_validator_error", "1"),
            ),
            True,
        )
        error_severity = (
            QualitySeverity.CRITICAL if block_on_error else QualitySeverity.HIGH
        )

        validators = build_validators(p_config) if p_config.get("enabled", True) else []
        if not validators:
            reason = (
                "No hay validaciones habilitadas para el dataset."
                if p_config.get("enabled", True)
                else "Validación deshabilitada en la configuración."
            )
            logger.info(f"'{ds}': {reason}")
            return emit_records_per_month(
                ds,
                [skipped_record(ds, reason)],
                list(p_month_ranges or [label_month_range(null_label)]),
                batch_months,
                p_sink,
            )

        columns = list(p_columns) if p_columns is not None else list(p_df.columns)
        for validator in validators:
            validator.bind_columns(columns)
        actual_date_col = find_column(columns, p_date_col) if p_date_col else None
        if p_date_col and not actual_date_col:
            logger.warning(
                f"Date column '{p_date_col}' not in {ds}; validating all rows as "
                f"one group."
            )
        month_expr = (
            f.last_day(f.to_date(f.col(actual_date_col)))
            if actual_date_col
            else f.lit(None).cast("date")
        )
        df_m = p_df.withColumn(MONTH_COL, month_expr)

        static, agg_groups, keyed = self._prepare(
            p_df, ds, validators, error_severity
        )

        with timed_phase(f"{ds} aggregate counts ({len(agg_groups)} validator(s))"):
            agg_rows, agg_failures = self._aggregate(df_m, agg_groups, ds)
        static += [
            validator_error_record(ds, type(v).__name__, error, error_severity)
            for v, error in agg_failures
        ]
        failed = [v for v, _ in agg_failures]
        agg_validators = [v for v, _ in agg_groups if v not in failed]

        months, month_rows, null_row = self._resolve_months(
            agg_rows, actual_date_col, p_month_ranges, null_label
        )

        null_records: List[ValidationRecord] = []
        if null_row and null_row.get(ROWS_ALIAS):
            logger.warning(
                f"'{ds}': {null_row[ROWS_ALIAS]:,} rows with null "
                f"'{actual_date_col}' validated as '{NULL_MONTH_LABEL}'."
            )
            extra = self._keyed_extra(
                self._run_keyed(
                    df_m.filter(f.col(MONTH_COL).isNull()),
                    keyed,
                    ds,
                    error_severity,
                    f"{ds} {NULL_MONTH_LABEL}",
                ),
                None,
            )
            null_records = stamp_records(
                assemble_month_records(
                    ds, null_row, static, agg_validators, extra, fail_on_empty
                ),
                *label_month_range(null_label),
            )
            null_summary = summarize_month(
                ds, NULL_MONTH_LABEL, null_records, len(validators), block_on_error
            )

        # Without a date column every month has the same data, so the keyed
        # validators run once and their records are reused.
        shared_extra: Optional[List[ValidationRecord]] = None
        if not actual_date_col and (agg_rows.get(None) or {}).get(ROWS_ALIAS):
            shared_extra = self._keyed_extra(
                self._run_keyed(df_m, keyed, ds, error_severity, f"{ds} all rows"),
                None,
            )

        summaries: List[Dict[str, Any]] = []
        blocks = split_into_blocks(months, batch_months)
        # Type of the date column, read once for every block filter below.
        date_kind: Optional[str] = None

        # Rows without a month go with the block holding their stamp month, so
        # one write replaces that month's results instead of two competing ones.
        null_block_idx = len(blocks) - 1
        for idx, block in enumerate(blocks):
            if any(start <= null_label <= end for start, end in block):
                null_block_idx = idx

        for b_idx, block in enumerate(blocks):
            block_start, block_end = block[0][0], block[-1][1]
            label = (
                f"{ds} block {b_idx + 1}/{len(blocks)} ({block_start} → {block_end})"
            )
            has_data = any(
                (month_rows.get(end) or {}).get(ROWS_ALIAS) for _, end in block
            )

            keyed_results: Dict[Optional[str], List[ValidationRecord]] = {}
            if keyed and has_data and actual_date_col:
                if date_kind is None:
                    try:
                        date_kind = LayerUtils._column_kind(p_df.schema, actual_date_col)
                    except Exception:
                        date_kind = ""
                keyed_results = self._run_keyed(
                    df_m.filter(
                        LayerUtils._date_range_condition(
                            date_kind, actual_date_col, block_start, block_end
                        )
                    ),
                    keyed,
                    ds,
                    error_severity,
                    label,
                )

            block_records: List[ValidationRecord] = []
            for m_start, m_end in block:
                row = month_rows.get(m_end)
                extra: List[ValidationRecord] = []
                if shared_extra is not None:
                    extra = [replace(r) for r in shared_extra]
                elif row and row.get(ROWS_ALIAS):
                    extra = self._keyed_extra(keyed_results, m_end)

                records = stamp_records(
                    assemble_month_records(
                        ds, row, static, agg_validators, extra, fail_on_empty
                    ),
                    m_start,
                    m_end,
                )
                summaries.append(
                    summarize_month(ds, m_end, records, len(validators), block_on_error)
                )
                block_records.extend(records)

            if null_records and b_idx == null_block_idx:
                block_records.extend(null_records)
                summaries.append(null_summary)

            with timed_phase(f"{label} persist {len(block_records)} result(s)"):
                p_sink(block_records)

        if null_records and not blocks:
            summaries.append(null_summary)
            with timed_phase(f"{ds} {NULL_MONTH_LABEL} persist results"):
                p_sink(null_records)

        total_rows = sum((r or {}).get(ROWS_ALIAS) or 0 for r in agg_rows.values())
        stop_months = sum(1 for s in summaries if s["decision"] == "STOP")
        logger.info(
            f"[SC-SUMMARY] {ds}: rows={total_rows:,} months={len(months)} "
            f"blocks={len(blocks)} batch_months={batch_months} "
            f"stop_months={stop_months} "
            f"elapsed={time.perf_counter() - started:.1f}s"
        )
        return summaries

    # -- preparation ------------------------------------------------------- #

    @staticmethod
    def _prepare(
        p_df: DataFrame,
        p_dataset_name: str,
        p_validators: Sequence[BaseValidator],
        p_error_severity: QualitySeverity,
    ) -> Tuple[
        List[ValidationRecord],
        List[Tuple[BaseValidator, List[Column]]],
        List[Tuple[BaseValidator, List[str]]],
    ]:
        """Split validators by grouped mode and collect data-independent records.

        A validator whose preparation raises (e.g. invalid config) becomes a
        VALIDATOR_ERROR record for every month and is not run.
        """
        static: List[ValidationRecord] = []
        agg_groups: List[Tuple[BaseValidator, List[Column]]] = []
        keyed: List[Tuple[BaseValidator, List[str]]] = []

        for idx, validator in enumerate(p_validators):
            try:
                if validator.GROUPED_MODE is GroupedMode.AGGREGATE:
                    exprs, records = validator.prepare_aggregations(
                        p_df, p_dataset_name, f"__v{idx}_"
                    )
                    static.extend(records)
                    if exprs:
                        agg_groups.append((validator, exprs))
                else:
                    keys, records = validator.prepare_keys(p_df, p_dataset_name)
                    static.extend(records)
                    if keys:
                        keyed.append((validator, keys))
            except Exception as e:
                logger.error(
                    f"Validator {type(validator).__name__} could not be prepared "
                    f"for {p_dataset_name}: {e}"
                )
                static.append(
                    validator_error_record(
                        p_dataset_name, type(validator).__name__, e, p_error_severity
                    )
                )
        return static, agg_groups, keyed

    # -- aggregate counts -------------------------------------------------- #

    @staticmethod
    def _collect_by_month(
        p_df_m: DataFrame, p_exprs: Sequence[Column]
    ) -> Dict[Optional[str], Dict[str, Any]]:
        """Run the per-month aggregation (split if very wide) and index by month."""
        rows: Dict[Optional[str], Dict[str, Any]] = {}
        chunks = [
            list(p_exprs[i : i + MAX_AGG_EXPRS_PER_JOB])
            for i in range(0, len(p_exprs), MAX_AGG_EXPRS_PER_JOB)
        ] or [[]]
        for chunk in chunks:
            result = (
                p_df_m.groupBy(MONTH_COL)
                .agg(f.count(f.lit(1)).alias(ROWS_ALIAS), *chunk)
                .collect()
            )
            for key, values in rows_by_month(result):
                rows.setdefault(key, {}).update(values)
        return rows

    def _aggregate(
        self,
        p_df_m: DataFrame,
        p_groups: Sequence[Tuple[BaseValidator, List[Column]]],
        p_dataset_name: str,
    ) -> Tuple[
        Dict[Optional[str], Dict[str, Any]], List[Tuple[BaseValidator, Exception]]
    ]:
        """Compute all aggregate metrics, isolating a failing validator.

        The combined job runs first; only if it fails is each validator run on
        its own, so one bad expression does not hide the others' results.

        Raises:
            Exception: If even the plain row count fails (data unreadable).
        """
        all_exprs = [expr for _, exprs in p_groups for expr in exprs]
        try:
            return self._collect_by_month(p_df_m, all_exprs), []
        except Exception as e:
            if not p_groups:
                raise
            logger.warning(
                f"Combined aggregation failed for {p_dataset_name} ({e}); "
                f"isolating validators."
            )

        rows = self._collect_by_month(p_df_m, [])
        failures: List[Tuple[BaseValidator, Exception]] = []
        for validator, exprs in p_groups:
            try:
                for key, values in self._collect_by_month(p_df_m, exprs).items():
                    rows.setdefault(key, {}).update(values)
            except Exception as e:
                logger.error(
                    f"Validator {type(validator).__name__} failed for "
                    f"{p_dataset_name}: {e}"
                )
                failures.append((validator, e))
        return rows, failures

    @staticmethod
    def _resolve_months(
        p_rows_by_month: Dict[Optional[str], Dict[str, Any]],
        p_actual_date_col: Optional[str],
        p_month_ranges: Optional[Sequence[MonthRange]],
        p_null_label: str,
    ) -> Tuple[
        List[MonthRange], Dict[str, Optional[Dict[str, Any]]], Optional[Dict[str, Any]]
    ]:
        """Decide which months are reported and which aggregate row each uses.

        Returns:
            Tuple of (months, aggregate row per month end, row of the
            null-month group when rows are attributed by date).
        """
        if not p_actual_date_col:
            whole = p_rows_by_month.get(None)
            months = list(p_month_ranges or [label_month_range(p_null_label)])
            return months, {end: whole for _, end in months}, None

        if p_month_ranges:
            months = list(p_month_ranges)
        else:
            present = sorted(k for k in p_rows_by_month if k is not None)
            months = (
                LayerUtils._resolve_month_ranges(f"{present[0][:7]}-01", present[-1])
                if present
                else []
            )

        null_row = p_rows_by_month.get(None)
        if not months and not (null_row and null_row.get(ROWS_ALIAS)):
            # Nothing at all: report the empty dataset once.
            months = [label_month_range(p_null_label)]

        return months, {end: p_rows_by_month.get(end) for _, end in months}, null_row

    # -- keyed validators -------------------------------------------------- #

    @staticmethod
    def _run_keyed(
        p_df_part: DataFrame,
        p_keyed: Sequence[Tuple[BaseValidator, List[str]]],
        p_dataset_name: str,
        p_error_severity: QualitySeverity,
        p_label: str,
    ) -> Dict[Optional[str], List[ValidationRecord]]:
        """Run KEYED validators on a block, grouped by month (one shuffle each).

        Returns:
            Records per month key; errors are stored under _ALL_MONTHS.
        """
        results: Dict[Optional[str], List[ValidationRecord]] = {}
        for validator, keys in p_keyed:
            name = type(validator).__name__
            try:
                with timed_phase(f"{p_label} {name}"):
                    rows = keyed_stats_frame(p_df_part, keys, [MONTH_COL]).collect()
                for key, values in rows_by_month(rows):
                    results.setdefault(key, []).extend(
                        validator.build_keyed_records(values, p_dataset_name)
                    )
            except Exception as e:
                logger.error(f"Validator {name} failed for {p_label}: {e}")
                results.setdefault(_ALL_MONTHS, []).append(
                    validator_error_record(p_dataset_name, name, e, p_error_severity)
                )
        return results

    @staticmethod
    def _keyed_extra(
        p_keyed_results: Mapping[Optional[str], List[ValidationRecord]],
        p_month_key: Optional[str],
    ) -> List[ValidationRecord]:
        """Keyed records of one month plus block-level errors (copied)."""
        return [
            replace(r)
            for r in list(p_keyed_results.get(p_month_key, []))
            + list(p_keyed_results.get(_ALL_MONTHS, []))
        ]

# --------------------------------------------------------------------------- #
# Dataset orchestration (sanity_check_flow)
# --------------------------------------------------------------------------- #


def resolve_dataset_config(
    p_metadata: Dict[str, Any], p_config_all: Dict[str, Any]
) -> Tuple[str, Optional[Dict[str, Any]]]:
    """Find a dataset's config; a config with _config_file renames the dataset."""
    dataset_name = p_metadata.get("dataset_name", "unknown")
    config = LayerUtils.get_dataset_config(p_config_all, dataset_name)

    if config and "_config_file" in config:
        for key, val in p_config_all.items():
            if val is config:
                dataset_name = key
                p_metadata["dataset_name"] = key
                break
    return dataset_name, config


def validate_dataset_grouped(
    p_metadata: Dict[str, Any],
    p_config_all: Dict[str, Any],
    p_loader_func: Callable,
    p_month_ranges: Sequence[MonthRange],
    p_sink: RecordsSink,
) -> List[Dict[str, Any]]:
    """Load one dataset for the whole range and validate it month-grouped.

    Load or validation errors become CRITICAL records for every month not yet
    persisted; a failure of the sink itself is raised.

    Args:
        p_metadata: Dataset metadata (dataset_name, path or database, date_col)
        p_config_all: All dataset configurations of the layer
        p_loader_func: Callable (metadata, config, start_dt, end_dt) returning
            a DataFrame, or (DataFrame, columns)
        p_month_ranges: Months to validate
        p_sink: Persists a block of records

    Returns:
        Summaries of the months whose decision is STOP.
    """
    dataset_name, config = resolve_dataset_config(p_metadata, p_config_all)
    if config is None:
        logger.warning(f"⚠️ No config found for dataset '{dataset_name}'")
        return []

    months = list(p_month_ranges)
    tracking = TrackingSink(p_sink)
    category = "LOAD_ERROR"

    with timed_phase(f"{dataset_name} total"):
        try:
            with timed_phase(f"{dataset_name} load"):
                loaded = p_loader_func(p_metadata, config, months[0][0], months[-1][1])
                # A loader may also return the columns it already knows.
                df, known_columns = loaded if isinstance(loaded, tuple) else (loaded, None)
                if df is None:
                    raise ValueError("loader returned no DataFrame")
                date_col = LayerUtils._resolve_date_col(config, p_metadata)
                df, columns = select_checked_columns(
                    df, dataset_name, config, date_col, p_columns=known_columns
                )

            category = "VALIDATION_ERROR"
            summaries = GroupedValidationRunner().run(
                df, dataset_name, config, date_col, months, tracking, p_columns=columns
            )
        except Exception as e:
            batch_months = resolve_batch_months(config)
            summaries = record_failure(
                e, dataset_name, category, months, batch_months, tracking
            )

    return notify_stops(dataset_name, summaries)
