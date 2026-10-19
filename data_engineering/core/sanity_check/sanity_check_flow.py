import logging
import os
import re
from contextlib import contextmanager
from functools import lru_cache
from typing import Any, Callable, Dict, Iterator, List, Optional, Set, Tuple

from prefect import flow, get_run_logger, task  # type: ignore
from pyspark import StorageLevel  # type: ignore
from pyspark.sql import DataFrame  # type: ignore
from pyspark.sql import functions as f  # type: ignore

from data_engineering.core.sanity_check import results_writer
from data_engineering.core.sanity_check.base import QualityGateError, ValidationRecord
from data_engineering.core.sanity_check.engine import LayerUtils, stop_totals
from data_engineering.core.sanity_check.grouped import (
    GroupedValidationRunner,
    RecordsSink,
    TrackingSink,
    dataset_error_record,
    default_label_date,
    emit_records_per_month,
    label_month_range,
    notify_stops,
    select_checked_columns,
    skipped_record,
    validate_dataset_grouped,
)
from data_engineering.core.sanity_check.utils import (
    DEFAULT_LAYER,
    PREFIXES_FILE,
    config_path,
    get_root_dir,
    load_yaml_or_default,
    timed_phase,
)
from data_engineering.core.utils import import_flow_module, send_message_teams

logger = logging.getLogger(__name__)

FLOW_NAME_SUFFIX = "_flow"
# Planning step of each layer's sanity check flow, filled by
# create_sanity_check_flow: lets execute_sanity_check skip creating a flow run
# when the node has nothing to validate.
_FLOW_PLANS: Dict[str, Callable[[], Optional[Tuple[Dict[str, Any], list]]]] = {}


@contextmanager
def _env_overrides(p_values: Dict[str, Optional[str]]) -> Iterator[None]:
    """Set (or unset, for None) environment variables, restoring them on exit."""

    def _apply(p_env: Dict[str, Optional[str]]) -> None:
        for key, value in p_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    saved = {k: os.getenv(k) for k in p_values}
    _apply(p_values)
    try:
        yield
    finally:
        _apply(saved)


def execute_sanity_check(p_layer_name: str, p_nodes: list = None) -> None:
    """Execute the sanity check of a layer, scoped to a node when given.

    Args:
        p_layer_name: Layer name to validate (e.g. 'raw_data', 'data_cleaning').
        p_nodes: List of node names to scope the validation (isolated mode).

    Raises:
        QualityGateError: When the data is blocked by critical failures.
        RuntimeError: When the sanity check cannot run (system error).
    """
    node = p_nodes[0] if p_nodes else None
    with _env_overrides({"node": node, "flow": p_layer_name}):
        try:
            module_path = (
                f"data_engineering.{p_layer_name}.src.sanity_check."
                f"sanity_check_validation"
            )
            sanity_module = import_flow_module(module_path)

            if not hasattr(sanity_module, "sanity_check_flow"):
                raise AttributeError(f"sanity_check_flow not found in {module_path}")

            if _has_nothing_to_validate(p_layer_name):
                logger.info(
                    f"[{p_layer_name}] Sanity check skipped for node={node}: no "
                    f"configured dataset to validate."
                )
            else:
                sanity_module.sanity_check_flow(p_node=node)

        except Exception as e:
            if is_quality_gate_exception(e):
                raise QualityGateError(
                    f"Sanity check failed for {p_layer_name}: {e}"
                ) from e

            error_msg = f"\u2757\ufe0f SANITY CHECK FAILED ({p_layer_name})\nError: {e}"
            if os.getenv("env") in ("dev", "prod"):
                try:
                    send_message_teams(error_msg)
                except Exception as notify_error:
                    logger.error(f"Could not send Teams notification: {notify_error}")

            raise RuntimeError(
                f"\u274c SANITY CHECK failed for {p_layer_name}: {e}"
            ) from e


def _has_nothing_to_validate(p_layer_name: str) -> bool:
    """True when the layer flow would return without validating anything.
    """
    plan = _FLOW_PLANS.get(p_layer_name)
    if plan is None:
        return False
    try:
        return plan() is None
    except Exception:
        return False


_QUALITY_GATE_ERROR_PATTERN = re.compile(
    r"^(Quality Gate FAILED|"
    r"Data persistence blocked: pre-save sanity check|Sanity check failed)",
    re.IGNORECASE,
)


def is_quality_gate_exception(p_error: BaseException) -> bool:
    """Return True if the error, or any error in its cause chain, is a quality block.
    """
    error: Optional[BaseException] = p_error
    while error is not None:
        if isinstance(error, QualityGateError):
            return True
        error = error.__cause__
    return bool(_QUALITY_GATE_ERROR_PATTERN.match(str(p_error)))


def _with_persisted_observation_columns(
    p_df: DataFrame, p_partition_col: Optional[str]
) -> DataFrame:
    """Add the observation columns save_delta_file_task derives before writing.

    Args:
        p_df: Staged DataFrame
        p_partition_col: Partition column used by save_delta_file_task

    Returns:
        DataFrame with the derived observation columns (validation only).
    """
    if not p_partition_col or p_partition_col not in p_df.columns:
        return p_df
    if "_observ_start_dt" not in p_df.columns:
        p_df = p_df.withColumn(
            "_observ_start_dt", f.trunc(f.col(p_partition_col), "month")
        )
    if p_partition_col != "_observ_end_dt":
        p_df = p_df.withColumn("_observ_end_dt", f.last_day(f.col(p_partition_col)))
    return p_df


def _results_sink(p_layer: str) -> RecordsSink:
    """Build a sink that persists a block of records in the results table."""

    def _sink(p_records: List[ValidationRecord]) -> None:
        results_writer.write_results(p_records, p_layer)

    return _sink


ENV_RESULTS_FLUSH_RECORDS = "sanity_check_results_flush_records"
DEFAULT_RESULTS_FLUSH_RECORDS = 20000


class BufferedResultsSink:
    """Collects the result records of many datasets and persists them together.
    """

    def __init__(self, p_layer: str) -> None:
        self._sink = _results_sink(p_layer)
        self._records: List[ValidationRecord] = []
        self.writes = 0
        try:
            self._max_records = max(
                1,
                int(os.getenv(ENV_RESULTS_FLUSH_RECORDS) or DEFAULT_RESULTS_FLUSH_RECORDS),
            )
        except ValueError:
            self._max_records = DEFAULT_RESULTS_FLUSH_RECORDS

    def __call__(self, p_records: List[ValidationRecord]) -> None:
        self._records.extend(p_records)
        if len(self._records) >= self._max_records:
            self.flush()

    def flush(self) -> None:
        """Persist the buffered records (no-op when empty)."""
        if not self._records:
            return
        records, self._records = self._records, []
        with timed_phase(f"persist {len(records)} buffered result(s)"):
            self._sink(records)
        self.writes += 1


def _run_pre_save_validation(
    p_df: DataFrame,
    p_table_name: str,
    p_config: Dict[str, Any],
    p_layer: str,
    p_date_col: Optional[str],
    p_start_date: Optional[str],
    p_end_date: Optional[str],
) -> List[Dict[str, Any]]:
    """Validate every staged row month-grouped and persist the results.

    Every month of start_dt → end_dt is reported (an empty one as
    EMPTY_DATASET), plus any month the data has outside that range, so older
    results of those months are always replaced.

    Args:
        p_df: Staged DataFrame (with derived observation columns)
        p_table_name: Dataset name used in the result records
        p_config: Dataset validator configuration
        p_layer: Active layer (e.g. 'data_cleaning')
        p_date_col: Date column that attributes rows to months (may be None)
        p_start_date: start_dt from the environment (may be None)
        p_end_date: end_dt from the environment (month of rows without a date)

    Returns:
        Summaries of the months whose decision is STOP.
    """
    validation_df, columns = select_checked_columns(
        p_df, p_table_name, p_config, p_date_col
    )

    cache_mode = os.getenv("sanity_check_pre_save_cache", "disk").strip().lower()
    cached = cache_mode != "none"
    if cached:
        try:
            validation_df = validation_df.persist(StorageLevel.DISK_ONLY)
        except Exception as e:
            cached = False
            logger.warning(
                f"[PRE-SAVE SC] '{p_table_name}': persist not available ({e}); "
                f"validating without cache."
            )

    label_date = p_end_date or default_label_date()
    month_ranges = (
        LayerUtils._resolve_month_ranges(p_start_date, p_end_date)
        if p_start_date and p_end_date
        else None
    )
    tracking = TrackingSink(_results_sink(p_layer))
    try:
        with timed_phase(f"[PRE-SAVE SC] {p_table_name} total"):
            try:
                summaries = GroupedValidationRunner().run(
                    validation_df,
                    p_table_name,
                    p_config,
                    p_date_col,
                    month_ranges,
                    tracking,
                    p_null_month_label=label_date,
                    p_columns=columns,
                )
            except Exception as e:
                if tracking.failed:
                    raise
                logger.error(
                    f"[PRE-SAVE SC] Error validating '{p_table_name}': {e}",
                    exc_info=True,
                )

                summaries = emit_records_per_month(
                    p_table_name,
                    [dataset_error_record(p_table_name, "VALIDATION_ERROR", e)],
                    [label_month_range(label_date)],
                    1,
                    tracking,
                )
    finally:
        if cached:
            validation_df.unpersist()

    return notify_stops(p_table_name, summaries)


def pre_save_quality_checks(
    p_df: DataFrame,
    p_file_name: str,
    p_data_subfolder: str,
    p_output_domain: str,
    p_date_col: Optional[str] = None,
) -> None:
    """Run sanity check and quality gate validations before persisting a dataset.

    Args:
        p_df: The DataFrame about to be persisted.
        p_file_name: Output dataset / table name being saved.
        p_data_subfolder: Active layer folder (e.g. 'data_cleaning').
        p_output_domain: Domain being processed (e.g. 'customer').
        p_date_col: Partition/date column used to split validation by month.

    Raises:
        QualityGateError: When the pre-save sanity check blocks persistence.
    """
    if p_data_subfolder == "raw_data":
        return

    _, table_name = _get_prefixed_dataset(os.getenv("output_domain"), p_file_name)
    start_date = os.getenv("start_dt")
    end_date = os.getenv("end_dt")
    config_all = LayerUtils.load_layer_config(p_data_subfolder, p_output_domain)
    config = LayerUtils.get_dataset_config(config_all, table_name)

    if config:
        date_col = p_date_col or LayerUtils._resolve_date_col(config, {})
        stop_summaries = _run_pre_save_validation(
            _with_persisted_observation_columns(p_df, p_date_col),
            table_name,
            config,
            p_data_subfolder,
            date_col,
            start_date,
            end_date,
        )

        if stop_summaries:
            total_critical, total_errors = stop_totals(stop_summaries)
            failed_months = [str(s.get("month", "unknown")) for s in stop_summaries]
            raise QualityGateError(
                f"Data persistence blocked: pre-save sanity check failed for "
                f"'{table_name}' — {total_critical} CRITICAL failure(s), "
                f"{total_errors} validator error(s) in months: "
                f"{', '.join(failed_months)}. Staged data will be discarded."
            )

        logger.info(
            f"[PRE-SAVE SC] Validation PASSED for '{table_name}'. Proceeding to "
            f"persist."
        )

    else:
        # Nothing ever writes results for a dataset without config, so its
        # history cannot block it: record the missing control instead.
        logger.warning(
            f"[PRE-SAVE SC] No sanity check config for '{table_name}'; persisting "
            f"WITHOUT quality checks (recorded as NO_CHECKS)."
        )
        months = (
            LayerUtils._resolve_month_ranges(start_date, end_date)
            if start_date and end_date
            else [label_month_range(end_date or default_label_date())]
        )
        emit_records_per_month(
            table_name,
            [skipped_record(table_name, "Sin configuración de sanity check.")],
            months,
            len(months),
            _results_sink(p_data_subfolder),
        )


@task(name="check_quality_gate_task", tags=["quality", "gate"])
def check_quality_gate_task(
    p_dataset_name: str,
    p_start_date: str,
    p_end_date: str,
    p_layer: str = "raw_data",
) -> None:
    """Task to check if a specific dataset is allowed to proceed.

    Args:
        p_dataset_name: Name of the dataset to check
        p_start_date: Start date for range-based check (YYYY-MM-DD format)
        p_end_date: End date for range-based check (YYYY-MM-DD format)
        p_layer: Layer name

    Raises:
        QualityGateError: When the dataset has CRITICAL quality failures (blocks flow).
    """
    if not LayerUtils.check_quality_gate(
        p_dataset_name, p_start_date, p_end_date, p_layer=p_layer
    ):
        raise QualityGateError(
            f"Quality Gate FAILED: {p_dataset_name} has CRITICAL failures "
            f"in {p_layer} layer for {p_start_date} \u2192 {p_end_date}"
        )
    get_run_logger().info(
        f"[QG PASSED] {p_dataset_name} passed quality gate in {p_layer}"
    )


def _get_prefixed_dataset(
    output_domain: Optional[str], output_dataset: str, node_name: str = None
) -> Tuple[str, str]:
    """Get prefixed and unprefixed dataset names.

    Args:
        output_domain: Output domain from environment or node name.
        output_dataset: Raw dataset name.
        node_name: Optional node name to extract domain from.

    Returns:
        Tuple of (output_dataset, prefixed_output).
    """
    if not output_domain and node_name and "." in node_name:
        output_domain = node_name.split(".")[0]

    try:
        prefixes = load_yaml_or_default(config_path(PREFIXES_FILE), {})
        prefix = prefixes.get(output_domain)
        prefixed_output = f"{prefix}_{output_dataset}" if prefix else output_dataset
    except Exception:
        prefixed_output = output_dataset

    return output_dataset, prefixed_output


def _load_flow_io_config(io_config_path: str, node_name: str) -> Optional[dict]:
    """
    Load a node's entry from io_config.yml.

    Args:
        io_config_path: Path to the io_config.yml file
        node_name: Name of the node ('<domain>.<flow>_flow')

    Returns:
        The flow configuration if it has an Input section, else None.
    """
    io_config = load_yaml_or_default(io_config_path, {})
    io_key = node_name.split(".", 1)[1].removesuffix(FLOW_NAME_SUFFIX)
    flow_config = io_config.get(io_key) if io_config else None

    if isinstance(flow_config, dict) and "Input" in flow_config:
        return flow_config
    return None


def _add_output_datasets(
    layer: str, flow_config: dict, actual_datasets: Set[str], node_name: str = None
) -> Set[str]:
    """Add output datasets from flow_config if applicable.

    Args:
        layer: Data layer
        flow_config: Flow configuration dictionary
        actual_datasets: Set of actual datasets
        node_name: Optional name of the node (e.g., 'bel.bel_logins_flow')

    Returns:
        Updated set of actual datasets
    """
    if layer != "data_cleaning" or "Output" not in flow_config:
        return actual_datasets

    output_dataset = flow_config["Output"]
    output_domain = os.getenv("output_domain")
    output_dataset, prefixed_output = _get_prefixed_dataset(
        output_domain, output_dataset, node_name
    )

    return (actual_datasets or set()) | {output_dataset, prefixed_output}


@lru_cache(maxsize=None)
def resolve_node_dependencies(
    node_name: str, layer: str = DEFAULT_LAYER
) -> Optional[Set[str]]:
    """
    Resolve which datasets a specific node depends on (cached per process).

    Args:
        node_name: Name of the node
        layer: Data layer

    Returns:
        Set of dataset names the node depends on, or None on any problem
    """
    if "." not in node_name:
        return None
    try:
        base_dir = get_root_dir()
        flow_config = _load_flow_io_config(
            os.path.join(base_dir, layer, "conf", "io_config.yml"), node_name
        )
        if flow_config is None:
            return None

        actual_datasets = _resolve_abstract_inputs_to_datasets(
            flow_config["Input"], layer, base_dir
        )
        return _add_output_datasets(layer, flow_config, actual_datasets, node_name)
    except Exception:
        return None


def _resolve_abstract_inputs_to_datasets(
    abstract_inputs: list, layer: str, base_dir: str
) -> Optional[Set[str]]:
    """Resolve abstract input names to actual dataset names using data_catalog.yml.

    Args:
        abstract_inputs: List of abstract input names.
        layer: Layer name
        base_dir: Base directory for file paths

    Returns:
        Set of actual dataset names
    """
    catalog_path = os.path.join(base_dir, layer, "conf", "data_catalog.yml")

    data_catalog = load_yaml_or_default(catalog_path, {"Input": {}})
    catalog_inputs = data_catalog.get("Input", {})
    actual_datasets = set()

    for abstract_input in abstract_inputs:
        if abstract_input not in catalog_inputs:
            logger.warning(
                f"Abstract input '{abstract_input}' not in data_catalog.Input"
            )
            continue

        entry = catalog_inputs[abstract_input]
        if isinstance(entry, dict) and "path" in entry:
            path = entry["path"].rstrip("/")
            dataset_name = path.split("/")[-1]
            actual_datasets.add(dataset_name)
        else:
            logger.warning(f"Cannot extract path from catalog entry '{abstract_input}'")

    return actual_datasets if actual_datasets else set(abstract_inputs)


def create_sanity_check_flow(
    layer_name: str,
    tag_name: str,
    dependency_mapping: Optional[Dict[str, str]] = None,
    use_catalog: bool = False,
) -> Callable:
    """
    Generate a parameterized sanity check Prefect flow for any data layer.

    Args:
        layer_name: Name of the data layer.
        tag_name: Short tag for Prefect tasks.
        dependency_mapping: Optional dict mapping workflow contexts to dependency.
        use_catalog: If True, load metadata from downstream layer's catalog;
            configured datasets missing from it get the default metadata.

    Returns:
        A Prefect @flow decorated function ready to execute.
    """
    def _get_filtered_datasets(config_all: Dict[str, Any]) -> list:
        """Configured datasets, limited to the node's dependencies when isolated."""
        configured_datasets = list(config_all.keys())

        if not os.getenv("node"):
            return configured_datasets
        if layer_name in ("data_integration", "data_transformation"):
            return configured_datasets

        mapping_layer = (dependency_mapping or {}).get(
            os.getenv("workflow"), layer_name
        )
        node_dependencies = resolve_node_dependencies(
            os.getenv("node"), layer=mapping_layer
        )
        if not node_dependencies:
            return configured_datasets

        return [ds for ds in configured_datasets if ds in node_dependencies]

    def _default_metadata(dataset_names) -> List[Dict[str, Any]]:
        """Metadata of datasets read as <env>_<layer>.<name> tables."""
        env = os.getenv("env", "dev")
        return [
            {
                "dataset_name": dataset_name,
                "database": f"{env}_{layer_name}",
                "date_col": "_observ_end_dt",
            }
            for dataset_name in dataset_names
        ]

    def _get_datasets_metadata(configured_datasets: List[str]) -> List[Dict[str, Any]]:
        """Resolve datasets metadata from catalog or create defaults.

        Args:
            configured_datasets: List of dataset names from configuration.

        Returns:
            List of metadata dictionaries for all datasets.
        """
        if not use_catalog:
            return _default_metadata(configured_datasets)

        all_metadata = LayerUtils.load_metadata_from_catalog(
            p_catalog_layer="data_cleaning",
            p_data_layer=layer_name,
        )
        matched_metadata = [
            item for item in all_metadata if item["dataset_name"] in configured_datasets
        ]
        matched_names = {item["dataset_name"] for item in matched_metadata}
        missing_configs = set(configured_datasets) - matched_names

        matched_metadata.extend(_default_metadata(missing_configs))

        return matched_metadata


    _run_state: Dict[str, BufferedResultsSink] = {}

    @task(name="process_dataset_task", tags=["quality", tag_name])
    def process_dataset_task(
        meta: Dict,
        config_all: Dict,
        month_ranges: List[Tuple[str, str]],
    ) -> List[Dict[str, Any]]:
        """Validate one dataset for all months into the flow's results buffer.

        Args:
            meta: Metadata dictionary for the dataset.
            config_all: Full configuration dictionary for the layer.
            month_ranges: Months to validate.

        Returns:
            The STOP month summaries.
        """
        sink = TrackingSink(_run_state["sink"])
        return validate_dataset_grouped(meta, config_all, month_ranges, sink)

    def _run_dataset_validation(
        datasets_metadata: List[Dict[str, Any]],
        config_all: Dict[str, Any],
        p_start_dt: Optional[str],
        p_end_dt: Optional[str],
    ) -> List[Dict]:
        """Validate every dataset month-grouped, whatever the range size.

        Args:
            datasets_metadata: List of metadata for all datasets.
            config_all: Full configuration dictionary.
            p_start_dt: Validation start date.
            p_end_dt: Validation end date.

        Returns:
            Summaries of the months whose decision is STOP.
        """
        month_ranges = LayerUtils._resolve_month_ranges(p_start_dt, p_end_dt)
        critical_fails: List[Dict] = []
        buffer = _run_state["sink"] = BufferedResultsSink(layer_name)

        with timed_phase(f"[{layer_name}] sanity check flow"):
            try:
                for meta in datasets_metadata:
                    critical_fails.extend(
                        process_dataset_task(meta, config_all, month_ranges)
                    )
            except Exception:
                # Keep what was validated before the failure, without hiding it.
                try:
                    buffer.flush()
                except Exception as flush_error:
                    logger.error(
                        f"[{layer_name}] Could not persist buffered results: "
                        f"{flush_error}"
                    )
                raise
            buffer.flush()

        if buffer.writes == 0:
            # Nothing was validated: still initialize the results table.
            results_writer.ensure_results_table(layer_name)
        return critical_fails

    def _plan() -> Optional[Tuple[Dict[str, Any], List[Dict[str, Any]]]]:
        """Configuration and metadata of the datasets to validate, or None.

        Raises:
            ValueError: In isolated execution without a domain.
        """
        # 1. Execution context: a node '<domain>.<flow>' means isolated mode
        node = os.getenv("node")
        domain = node.split(".")[0] if node and "." in node else None
        logger.info(f"Execution context: node={node}, domain={domain}")
        if node is not None and not domain:
            raise ValueError("Isolated execution requires domain to be set")

        # 2. Load and filter configuration
        config_all = LayerUtils.load_layer_config(layer_name, domain)
        if not config_all:
            logger.warning(
                f"[{layer_name}] No sanity check config found "
                f"(domain={domain}). Nothing to validate or persist."
            )
            return None

        configured_datasets = _get_filtered_datasets(config_all)
        if not configured_datasets:
            logger.warning(
                f"[{layer_name}] No datasets matched after filtering "
                f"(node={os.getenv('node')}). Nothing to validate or persist."
            )
            return None

        # 3. Resolve metadata
        datasets_metadata = _get_datasets_metadata(configured_datasets)
        if not datasets_metadata:
            logger.warning(
                f"[{layer_name}] No dataset metadata resolved for "
                f"{configured_datasets}. Nothing to validate or persist."
            )
            return None
        return config_all, datasets_metadata

    _FLOW_PLANS[layer_name] = _plan

    @flow(name="sanity_check_flow")
    def sanity_check_flow(p_node: Optional[str] = None) -> None:
        """Validate data layer tables with context-aware execution.

        Args:
            p_node: Optional node name
        """
        try:
            if p_node:
                os.environ["node"] = p_node

            plan = _plan()
            if plan is None:
                return
            config_all, datasets_metadata = plan

            p_start_dt = os.getenv("start_dt")
            p_end_dt = os.getenv("end_dt")

            critical_fails = _run_dataset_validation(
                datasets_metadata, config_all, p_start_dt, p_end_dt
            )

            if critical_fails:
                failed_datasets = sorted(
                    {s.get("dataset_name", "unknown") for s in critical_fails}
                )
                raise QualityGateError(
                    f"Critical failures in {layer_name} datasets: "
                    f"{', '.join(failed_datasets)}"
                )

        except Exception as e:
            logger.error(f"❌ Sanity check flow failed for {layer_name}: {e}")
            raise

    return sanity_check_flow
