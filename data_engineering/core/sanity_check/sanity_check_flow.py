import logging
import os
import re
import threading
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from prefect import flow, get_run_logger, task  # type: ignore
from pyspark import StorageLevel  # type: ignore
from pyspark.sql import DataFrame  # type: ignore
from pyspark.sql import functions as f  # type: ignore

from data_engineering.core.sanity_check.base import ValidationRecord
from data_engineering.core.sanity_check.engine import (
    FlowExecutionContext,
    LayerUtils,
    get_spark,
)
from data_engineering.core.sanity_check.grouped import (
    GroupedValidationRunner,
    RecordsSink,
    TrackingSink,
    dataset_error_record,
    default_label_date,
    emit_records_per_month,
    label_month_range,
    validate_dataset_grouped,
)
from data_engineering.core.sanity_check.utils import (
    DEFAULT_LAYER,
    ConfigLoader,
    EnvironmentConfig,
    timed_phase,
    to_dataframe,
)
from data_engineering.core.utils import import_flow_module, send_message_teams

logger = logging.getLogger(__name__)

_UPSTREAM_MAP = {
    layer: [upstream]
    for layer, upstream in LayerUtils.LAYER_DEPENDENCIES.items()
    if upstream
}
FLOW_NAME_SUFFIX = "_flow"
_DEPENDENCY_CACHE: Dict[str, Optional[Set[str]]] = {}
_ENV_MUTATION_LOCK = threading.Lock()


def execute_sanity_check(
    p_layer_name: str, p_nodes: list = None, p_is_upstream: bool = False
) -> None:
    """Execute sanity check for a workflow, optionally traversing upstream dependencies.

    The `node` and `flow` environment variables are set for the duration of the
    check and restored afterwards, so the caller's context is left untouched.

    Args:
        p_layer_name: Layer name to validate (e.g. 'raw_data', 'data_cleaning').
        p_nodes: List of node names to scope the validation (isolated mode).
        p_is_upstream: If True, resolves and validates all upstream dependencies first.

    Raises:
        RuntimeError: When the sanity check or any upstream check fails.
    """
    if p_is_upstream:
        for upstream in _UPSTREAM_MAP.get(p_layer_name, []):
            execute_sanity_check(upstream, p_nodes, p_is_upstream=False)
            logger.info(f"{upstream} sanity_check PASSED")
        return

    with _ENV_MUTATION_LOCK:
        saved_env = {k: os.getenv(k) for k in ("node", "flow")}
        if p_nodes:
            os.environ["node"] = p_nodes[0]
        else:
            os.environ.pop("node", None)
        os.environ["flow"] = p_layer_name

    try:
        module_path = (
            f"data_engineering.{p_layer_name}.src.sanity_check.sanity_check_validation"
        )
        sanity_module = import_flow_module(module_path)

        if not hasattr(sanity_module, "sanity_check_flow"):
            raise AttributeError(f"sanity_check_flow not found in {module_path}")

        _node_arg = p_nodes[0] if p_nodes else None
        getattr(sanity_module, "sanity_check_flow")(p_node=_node_arg)
        os.environ["sanity_check_executed"] = "1"

    except Exception as e:
        env_flow = os.getenv("env")
        error_msg = f"\u2757\ufe0f SANITY CHECK FAILED ({p_layer_name})\nError: {e}"

        if env_flow in ("dev", "prod") and not is_quality_gate_error(str(e)):
            send_message_teams(error_msg)

        raise RuntimeError(f"\u274c SANITY CHECK failed for {p_layer_name}: {e}") from e
    finally:
        with _ENV_MUTATION_LOCK:
            for k, v in saved_env.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v


_QUALITY_GATE_ERROR_PATTERN = re.compile(
    r"^("
    r"Quality Gate FAILED|"
    r"Blocked by quality gate|"
    r"check_quality_gate|"
    r"sc_sanity_check_results|"
    r"Data persistence blocked: (pre-save sanity check|quality gate)|"
    r"Sanity check failed"
    r")",
    re.IGNORECASE,
)


def is_quality_gate_error(p_error_msg: str) -> bool:
    """Return True if a RuntimeError originated from the quality gate or sanity check.

    Args:
        p_error_msg: String representation of the RuntimeError.

    Returns:
        bool: True if error originated from quality gate/sanity check, False otherwise.
    """
    return bool(_QUALITY_GATE_ERROR_PATTERN.search(p_error_msg))


def prepare_flow_sanity_checks(
    p_layer_name: str, p_nodes: list = None, p_sanity_check: bool = False
) -> None:
    """Prepare environment and run upstream sanity checks before main flows execute.

    Args:
        p_layer_name: Active workflow layer name.
        p_nodes: List of nodes being executed; non-None means isolated mode.
        p_sanity_check: Whether sanity checks are enabled.
    """
    os.environ.pop("sanity_check_executed", None)

    # Disabled, unknown layer or isolated mode (p_nodes given): nothing deferred.
    if not p_sanity_check or not p_layer_name or p_nodes is not None:
        os.environ.pop("sanity_check_deferred", None)
        return

    if p_layer_name in ("data_cleaning", "data_transformation", "data_integration"):
        os.environ["sanity_check_deferred"] = "1"
    elif p_layer_name in _UPSTREAM_MAP:
        logger.info(
            f"[PER-NODE QG] '{p_layer_name}': solo QG sobre resultados upstream "
            "existentes. Sin ejecución de SC upstream."
        )


def _with_persisted_observation_columns(
    p_df: DataFrame, p_partition_col: Optional[str]
) -> DataFrame:
    """Add the observation columns save_delta_file_task derives before writing.

    Mirrors save_data_flow.save_delta_file_task so pre-save validation sees the
    same `_observ_start_dt` / `_observ_end_dt` that will be persisted (configs
    commonly check them for nulls).

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
        validation_results(to_dataframe(get_spark(), p_records), layer=p_layer)

    return _sink


def _run_pre_save_validation(
    p_df: DataFrame,
    p_table_name: str,
    p_config: Dict[str, Any],
    p_layer: str,
    p_date_col: Optional[str],
    p_end_date: Optional[str],
) -> List[Dict[str, Any]]:
    """Validate every staged row month-grouped and persist the results.

    Only the columns the checks read are kept, and by default they are
    materialized once with DISK_ONLY: the aggregate pass and the duplicate
    blocks then read that copy instead of recomputing the whole transformation
    each time, without using executor memory. Set the environment variable
    `sanity_check_pre_save_cache=none` to recompute instead (no local disk).

    Months come from the data itself; rows with a null date are validated as
    their own group, stamped with end_dt.

    Args:
        p_df: Staged DataFrame (with derived observation columns)
        p_table_name: Dataset name used in the result records
        p_config: Dataset validator configuration
        p_layer: Active layer (e.g. 'data_cleaning')
        p_date_col: Date column that attributes rows to months (may be None)
        p_end_date: end_dt from the environment (stamp for rows without month)

    Returns:
        Summaries of the months whose decision is STOP.
    """
    required_cols = LayerUtils._extract_columns_for_caching(
        p_config, p_date_col, p_df.columns
    )
    if len(required_cols) < len(p_df.columns):
        logger.info(
            f"[PRE-SAVE SC] '{p_table_name}': validating "
            f"{len(required_cols)}/{len(p_df.columns)} required columns."
        )
        validation_df = p_df.select(*required_cols)
    else:
        validation_df = p_df

    cache_mode = os.getenv("sanity_check_pre_save_cache", "disk").strip().lower()
    cached = cache_mode != "none"
    if cached:
        validation_df = validation_df.persist(StorageLevel.DISK_ONLY)

    label_date = p_end_date or default_label_date()
    tracking = TrackingSink(_results_sink(p_layer))
    try:
        with timed_phase(f"[PRE-SAVE SC] {p_table_name} total"):
            try:
                summaries = GroupedValidationRunner(p_layer).run(
                    validation_df,
                    p_table_name,
                    p_config,
                    p_date_col,
                    None,
                    tracking,
                    p_null_month_label=label_date,
                )
            except Exception as e:
                if tracking.failed:
                    raise
                logger.error(
                    f"[PRE-SAVE SC] Error validating '{p_table_name}': {e}",
                    exc_info=True,
                )
                # Always recorded, even if this month was already written:
                # the staged data must be blocked.
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

    stops = [s for s in summaries if s.get("decision") == "STOP"]
    LayerUtils._send_stop_notification(p_table_name, stops)
    return stops


def pre_save_quality_checks(
    p_df: DataFrame,
    p_file_name: str,
    p_data_subfolder: str,
    p_output_domain: str,
    p_date_col: Optional[str] = None,
) -> None:
    """Run sanity check and quality gate validations before persisting a dataset.

    When the dataset has a sanity check config, the staged data is validated
    and that fresh result is authoritative: the historical quality gate is
    not consulted, since its rows describe the data being replaced. Without a
    config, the historical gate is the only control applied.

    Args:
        p_df: The DataFrame about to be persisted.
        p_file_name: Output dataset / table name being saved.
        p_data_subfolder: Active layer folder (e.g. 'data_cleaning').
        p_output_domain: Domain being processed (e.g. 'customer').
        p_date_col: Partition/date column used to split validation by month.

    Raises:
        RuntimeError: When sanity check fails or quality gate blocks persistence.
    """
    if p_output_domain == "sanity_check":
        return

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
            end_date,
        )

        if stop_summaries:
            total_critical = sum(s.get("critical_failures", 0) for s in stop_summaries)
            total_errors = sum(s.get("validator_errors", 0) for s in stop_summaries)
            failed_months = [str(s.get("month", "unknown")) for s in stop_summaries]
            raise RuntimeError(
                f"Data persistence blocked: pre-save sanity check failed for "
                f"'{table_name}' — {total_critical} CRITICAL failure(s), "
                f"{total_errors} validator error(s) in months: "
                f"{', '.join(failed_months)}. Staged data will be discarded."
            )

        logger.info(
            f"[PRE-SAVE SC] Validation PASSED for '{table_name}'. Proceeding to "
            f"persist."
        )

    elif start_date and end_date:
        logger.info(
            f"[PRE-SAVE SC] No sanity check config for '{table_name}'; checking "
            f"quality gate history ({start_date} → {end_date})..."
        )
        if not LayerUtils.check_quality_gate(
            table_name, start_date, end_date, p_layer=p_data_subfolder
        ):
            logger.error(
                f"[PRE-SAVE SC] Quality Gate FAILED for '{table_name}' "
                f"({start_date} → {end_date}). CRITICAL failures detected."
            )
            raise RuntimeError(
                f"Blocked by quality gate: "
                f"{table_name} {start_date} → {end_date}"
            )
        logger.info(
            f"[PRE-SAVE SC] Quality Gate PASSED for '{table_name}' "
            f"({start_date} → {end_date}). Safe to persist."
        )
    else:
        logger.warning(
            f"[PRE-SAVE SC] No sanity check config for '{table_name}' and no "
            f"start_dt/end_dt; persisting without quality checks."
        )

    os.environ["sanity_check_executed"] = "1"


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
        RuntimeError: When the dataset has CRITICAL quality failures (blocks flow).
    """
    # check_quality_gate handles its own errors (it fails closed), so only
    # its result needs to be turned into a blocking error.
    if not LayerUtils.check_quality_gate(
        p_dataset_name, p_start_date, p_end_date, p_layer=p_layer
    ):
        raise RuntimeError(
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
        prefix_path = os.path.join(
            EnvironmentConfig.get_root_dir(), "env", "base", "prefixes.yml"
        )
        prefixes = ConfigLoader.load_yaml(
            prefix_path, "prefixes.yml", p_fallback_value={}
        )
        prefix = prefixes.get(output_domain)
        prefixed_output = f"{prefix}_{output_dataset}" if prefix else output_dataset
    except Exception:
        prefixed_output = output_dataset

    return output_dataset, prefixed_output


def validation_results(df_results: DataFrame, layer: str = DEFAULT_LAYER) -> None:
    """
    Persist validation results using unified save_data_flow.

    Args:
        df_results: DataFrame with validation records.
        layer: Data layer.
    """
    env_overrides = {
        "folder": layer,
        "flow_key": "sanity_check_results",
        "output_domain": "sanity_check",
        "env": os.getenv("env", "dev"),
        "auto_sanity_check_layer": layer,
        "sanity_check_executed": "1",
    }

    with _ENV_MUTATION_LOCK:
        original_env = {k: os.getenv(k) for k in env_overrides.keys()}
        os.environ.update(env_overrides)

        try:
            from data_engineering.core.save_data_flow import save_data_flow

            save_data_flow(df_results, p_mode="MERGE_BY_DATASET")
        except Exception as e:
            logger.error(
                f"❌ Failed to persist validation results for {layer}: {e}",
                exc_info=True,
            )
            raise
        finally:
            for k, v in original_env.items():
                if v is not None:
                    os.environ[k] = v
                else:
                    os.environ.pop(k, None)


def _load_flow_io_config(io_config_path: str, node_name: str) -> Optional[dict]:
    """
    Load a node's entry from io_config.yml.

    Args:
        io_config_path: Path to the io_config.yml file
        node_name: Name of the node ('<domain>.<flow>_flow')

    Returns:
        The flow configuration if it has an Input section, else None.
    """
    io_config = ConfigLoader.load_yaml(
        io_config_path, "io_config.yml", p_fallback_value={}
    )
    flow_name = node_name.split(".", 1)[1]
    io_key = (
        flow_name.replace(FLOW_NAME_SUFFIX, "")
        if flow_name.endswith(FLOW_NAME_SUFFIX)
        else flow_name
    )
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


def resolve_node_dependencies(
    node_name: str, layer: str = DEFAULT_LAYER
) -> Optional[Set[str]]:
    """
    Resolve which datasets a specific node depends on.

    Args:
        node_name: Name of the node
        layer: Data layer

    Returns:
        Set of dataset names the node depends on, or None if not found
    """
    cache_key = f"{layer}:{node_name}"
    if cache_key not in _DEPENDENCY_CACHE:
        _DEPENDENCY_CACHE[cache_key] = _compute_node_dependencies(node_name, layer)
    return _DEPENDENCY_CACHE[cache_key]


def _compute_node_dependencies(node_name: str, layer: str) -> Optional[Set[str]]:
    """Uncached body of resolve_node_dependencies (None on any problem)."""
    if "." not in node_name:
        return None
    try:
        base_dir = EnvironmentConfig.get_root_dir()
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

    data_catalog = ConfigLoader.load_yaml(
        catalog_path, "data_catalog.yml", p_fallback_value={"Input": {}}
    )
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
    loader_func: Callable,
    tag_name: str,
    db_suffix: str,
    dependency_mapping: Optional[Dict[str, str]] = None,
    use_catalog: bool = False,
    create_missing_metadata_func: Optional[Callable] = None,
) -> Callable:
    """
    Generate a parameterized sanity check Prefect flow for any data layer.

    Args:
        layer_name: Name of the data layer.
        loader_func: Callable to load datasets.
        tag_name: Short tag for Prefect tasks.
        db_suffix: Database suffix for the layer.
        dependency_mapping: Optional dict mapping workflow contexts to dependency.
        use_catalog: If True, load metadata from downstream layer's catalog.
        create_missing_metadata_func: Optional function to create fallback metadata.

    Returns:
        A Prefect @flow decorated function ready to execute.
    """

    def _get_filtered_datasets(config_all: Dict[str, Any], context) -> list:
        """Get filtered list of datasets based on execution context."""
        configured_datasets = list(config_all.keys())

        if not (context.is_isolated_execution and os.getenv("node")):
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

    def _get_datasets_metadata(configured_datasets: List[str]) -> List[Dict[str, Any]]:
        """Resolve datasets metadata from catalog or create defaults.

        Args:
            configured_datasets: List of dataset names from configuration.

        Returns:
            List of metadata dictionaries for all datasets.
        """
        if not use_catalog:
            env = os.getenv("env", "dev")
            return [
                {
                    "dataset_name": dataset_name,
                    "database": f"{env}_{db_suffix}",
                    "date_col": "_observ_end_dt",
                }
                for dataset_name in configured_datasets
            ]

        all_metadata = LayerUtils.load_metadata_from_catalog(
            p_catalog_layer="data_cleaning",
            p_data_layer=layer_name,
        )
        matched_metadata = [
            item for item in all_metadata if item["dataset_name"] in configured_datasets
        ]
        matched_names = {item["dataset_name"] for item in matched_metadata}
        missing_configs = set(configured_datasets) - matched_names

        if missing_configs and create_missing_metadata_func:
            matched_metadata.extend(create_missing_metadata_func(missing_configs))

        return matched_metadata

    @task(name="process_dataset_task", tags=["quality", tag_name])
    def process_dataset_task(
        meta: Dict,
        config_all: Dict,
        month_ranges: List[Tuple[str, str]],
    ) -> Dict[str, Any]:
        """Validate one dataset for all months, persisting results per block.

        Args:
            meta: Metadata dictionary for the dataset.
            config_all: Full configuration dictionary for the layer.
            month_ranges: Months to validate.

        Returns:
            Dict with the STOP month summaries ("stops") and the number of
            result writes ("writes").
        """
        sink = TrackingSink(_results_sink(layer_name))
        stops = validate_dataset_grouped(
            meta, config_all, loader_func, layer_name, month_ranges, sink
        )
        return {"stops": stops, "writes": sink.calls}

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
        writes = 0

        with timed_phase(f"[{layer_name}] sanity check flow"):
            for meta in datasets_metadata:
                outcome = process_dataset_task(meta, config_all, month_ranges)
                critical_fails.extend(outcome["stops"])
                writes += outcome["writes"]

        if writes == 0:
            # Nothing was validated: still initialize the results table.
            validation_results(to_dataframe(get_spark(), []), layer=layer_name)
        return critical_fails

    @flow(name="sanity_check_flow")
    def sanity_check_flow(p_node: Optional[str] = None) -> None:
        """Validate data layer tables with context-aware execution.

        Args:
            p_node: Optional node name
        """
        try:
            if p_node:
                os.environ["node"] = p_node

            # 1. Capture context
            context = FlowExecutionContext.from_environment()
            if context.is_isolated_execution and not context.domain:
                raise ValueError("Isolated execution requires domain to be set")

            # 2. Load and filter configuration
            config_all = LayerUtils.load_layer_config(layer_name, context.domain)
            if not config_all:
                logger.warning(
                    f"[{layer_name}] No sanity check config found "
                    f"(domain={context.domain}). Nothing to validate or persist."
                )
                return

            configured_datasets = _get_filtered_datasets(config_all, context)
            if not configured_datasets:
                logger.warning(
                    f"[{layer_name}] No datasets matched after filtering "
                    f"(node={os.getenv('node')}). Nothing to validate or persist."
                )
                return

            # 3. Resolve metadata
            datasets_metadata = _get_datasets_metadata(configured_datasets)

            if not datasets_metadata:
                logger.warning(
                    f"[{layer_name}] No dataset metadata resolved for "
                    f"{configured_datasets}. Nothing to validate or persist."
                )
                return

            # 4. Validate all datasets
            p_start_dt = os.getenv("start_dt")
            p_end_dt = os.getenv("end_dt")

            critical_fails = _run_dataset_validation(
                datasets_metadata, config_all, p_start_dt, p_end_dt
            )

            if critical_fails:
                failed_datasets = sorted(
                    {s.get("dataset_name", "unknown") for s in critical_fails}
                )
                raise RuntimeError(
                    f"Critical failures in {layer_name} datasets: "
                    f"{', '.join(failed_datasets)}"
                )

        except Exception as e:
            logger.error(f"❌ Sanity check flow failed for {layer_name}: {e}")
            raise

    return sanity_check_flow
