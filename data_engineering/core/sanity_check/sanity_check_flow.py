import logging
import os
import re
import threading
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from prefect import flow, get_run_logger, task  # type: ignore
from pyspark import StorageLevel  # type: ignore
from pyspark.sql import DataFrame  # type: ignore

from data_engineering.core.sanity_check.engine import (
    FlowExecutionContext,
    LayerUtils,
    QualityEngine,
    df_is_empty,
)
from data_engineering.core.sanity_check.utils import DEFAULT_LAYER, ConfigLoader
from data_engineering.core.utils import import_flow_module, send_message_teams

logger = logging.getLogger(__name__)

_UPSTREAM_MAP = {
    "data_cleaning": ["raw_data"],
    "data_transformation": ["data_cleaning"],
    "data_integration": ["data_transformation"],
}
_PRE_SAVE_MONTH_THRESHOLD = 3
FLOW_NAME_SUFFIX = "_flow"
_DEPENDENCY_CACHE: Dict[str, Optional[Set[str]]] = {}
_ENV_MUTATION_LOCK = threading.Lock()


def execute_sanity_check(
    p_layer_name: str, p_nodes: list = None, p_is_upstream: bool = False
) -> None:
    """Execute sanity check for a workflow, optionally traversing upstream dependencies.

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
            print(f"{upstream} sanity_check PASSED\n")
        return

    try:
        if p_nodes:
            os.environ["node"] = p_nodes[0]
        else:
            os.environ.pop("node", None)

        module_path = (
            f"data_engineering.{p_layer_name}.src.sanity_check.sanity_check_validation"
        )
        sanity_module = import_flow_module(module_path)

        if hasattr(sanity_module, "sanity_check_flow"):
            os.environ["flow"] = p_layer_name
            _node_arg = p_nodes[0] if p_nodes else None
            getattr(sanity_module, "sanity_check_flow")(p_node=_node_arg)
            os.environ["sanity_check_executed"] = "1"
        else:
            raise AttributeError(f"sanity_check_flow not found in {module_path}")

    except Exception as e:
        env_flow = os.getenv("env")
        error_type = "UPSTREAM SANITY CHECK" if p_is_upstream else "SANITY CHECK"
        error_msg = f"\u2757\ufe0f {error_type} FAILED ({p_layer_name})\nError: {e}"

        from data_engineering.core.sanity_check.sanity_check_flow import (
            is_quality_gate_error,
        )

        if env_flow in ("dev", "prod") and not is_quality_gate_error(str(e)):
            send_message_teams(error_msg)

        raise RuntimeError(f"\u274c {error_type} failed for {p_layer_name}: {e}")


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

    is_isolated_mode = p_nodes is not None

    if not p_sanity_check or not p_layer_name:
        os.environ.pop("sanity_check_deferred", None)
        return

    if is_isolated_mode:
        os.environ.pop("sanity_check_deferred", None)
        return

    if p_layer_name in ("data_cleaning", "data_transformation", "data_integration"):
        os.environ["sanity_check_deferred"] = "1"
    elif p_layer_name in _UPSTREAM_MAP:
        print(
            f"[PER-NODE QG] '{p_layer_name}': solo QG sobre resultados upstream "
            "existentes. Sin ejecución de SC upstream."
        )


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
        RuntimeError: When sanity check fails or quality gate blocks persistence.
    """
    if p_output_domain == "sanity_check":
        return

    if p_data_subfolder == "raw_data":
        return

    _, table_name = _get_prefixed_dataset(os.getenv("output_domain"), p_file_name)
    start_date = os.getenv("start_dt")
    end_date = os.getenv("end_dt")
    load_config = LayerUtils.create_layer_config_loader(p_data_subfolder)
    config_all = load_config(p_domain_filter_override=p_output_domain)

    if config_all:
        config = LayerUtils.get_dataset_config(config_all, table_name)
        if config:
            month_ranges = LayerUtils._resolve_month_ranges(start_date, end_date)
            engine = QualityEngine()
            stop_summaries = []

            def _pre_save_validate(p_name, p_month_df, p_cfg):
                return engine.validate(
                    p_month_df, p_name, p_cfg, p_layer=p_data_subfolder
                )

            date_col_to_use = p_date_col or LayerUtils._resolve_date_col(config, {})
            required_cols = LayerUtils._extract_columns_for_caching(
                config, date_col_to_use, p_df.columns
            )

            if len(required_cols) < len(p_df.columns):
                logger.info(
                    f"[PRE-SAVE SC] '{table_name}': optimizing validation by selecting "
                    f"{len(required_cols)}/{len(p_df.columns)} required columns."
                )
                validation_df = p_df.select(*required_cols)
            else:
                validation_df = p_df

            if not p_date_col:
                full_df = validation_df.persist(StorageLevel.MEMORY_AND_DISK)
                try:
                    results_df, summary = engine.validate(
                        full_df, table_name, config, p_layer=p_data_subfolder
                    )
                    if results_df is not None and not df_is_empty(results_df):
                        validation_results(results_df, layer=p_data_subfolder)
                    if summary.get("decision") == "STOP":
                        summary["month"] = end_date or "ALL"
                        stop_summaries.append(summary)
                finally:
                    full_df.unpersist()
            else:
                yearly_chunks = LayerUtils._group_month_ranges_by_year(month_ranges)

                for year_chunk in yearly_chunks:
                    chunk_start = year_chunk[0][0]
                    chunk_end = year_chunk[-1][1]

                    logger.info(
                        f"[PRE-SAVE SC] Processing year chunk: {chunk_start[:4]} "
                        f"({len(year_chunk)} months)"
                    )

                    chunk_df = LayerUtils._apply_date_filter(
                        validation_df,
                        p_date_col,
                        chunk_start,
                        chunk_end,
                        table_name,
                    ).persist(StorageLevel.MEMORY_AND_DISK)

                    batch_results = []
                    try:
                        (
                            batch_results,
                            chunk_stops,
                        ) = LayerUtils._validate_df_over_months(
                            chunk_df,
                            table_name,
                            config,
                            _pre_save_validate,
                            p_date_col,
                            year_chunk,
                        )
                        stop_summaries.extend(chunk_stops)

                        if batch_results:
                            consolidated_batch = LayerUtils.consolidate_results(
                                batch_results
                            )
                            validation_results(
                                consolidated_batch, layer=p_data_subfolder
                            )
                            logger.info(
                                f"[PRE-SAVE SC] Persisted results {chunk_start[:4]} "
                                f"({len(batch_results)} results)."
                            )

                    finally:
                        chunk_df.unpersist()
                        for df in batch_results:
                            try:
                                df.unpersist()
                            except Exception:
                                pass
                        batch_results.clear()

            if stop_summaries:
                total_critical = sum(
                    s.get("critical_failures", 0) for s in stop_summaries
                )
                failed_months = [s.get("month", "unknown") for s in stop_summaries]
                raise RuntimeError(
                    f"Data persistence blocked: pre-save sanity check failed for "
                    f"'{table_name}' — {total_critical} CRITICAL failure(s) in "
                    f"months: {', '.join(failed_months)}. Staged data will be "
                    f"discarded."
                )

            logger.info(
                f"[PRE-SAVE SC] Validation PASSED for '{table_name}' "
                f"({len(month_ranges)} month(s)). Proceeding to persist."
            )

    if start_date and end_date:
        logger.info(
            f"[PRE-SAVE SC] Checking quality gate for '{table_name}' "
            f"({start_date} \u2192 {end_date})..."
        )
        if not LayerUtils.check_quality_gate(
            table_name, start_date, end_date, p_layer=p_data_subfolder
        ):
            logger.error(
                f"[PRE-SAVE SC] Quality Gate FAILED for '{table_name}' "
                f"({start_date} \u2192 {end_date}). CRITICAL failures detected."
            )
            raise RuntimeError(
                f"Blocked by quality gate: "
                f"{table_name} {start_date} \u2192 {end_date}"
            )
        print(
            f"[PRE-SAVE SC] Quality Gate PASSED for '{table_name}' "
            f"({start_date} \u2192 {end_date}). Safe to persist."
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
    _logger = get_run_logger()

    try:
        passed = LayerUtils.check_quality_gate(
            p_dataset_name, p_start_date, p_end_date, p_layer=p_layer
        )
        if not passed:
            raise RuntimeError(
                f"Quality Gate FAILED: {p_dataset_name} has CRITICAL failures "
                f"in {p_layer} layer for {p_start_date} \u2192 {p_end_date}"
            )
        _logger.info(f"[QG PASSED] {p_dataset_name} passed quality gate in {p_layer}")
    except RuntimeError:
        raise
    except Exception as e:
        if LayerUtils._is_missing_table_error(str(e)):
            _logger.info(
                f"[TABLE NOT FOUND] Quality gate table not yet created for {p_layer}. "
                f"Allowing {p_dataset_name} to proceed."
            )
            return

        _logger.error(
            f"[QG ERROR] Could not verify quality gate for {p_dataset_name} "
            f"in {p_layer}. Blocking by default. Error: {e}"
        )
        raise RuntimeError(
            f"Quality Gate verification error for {p_dataset_name} in {p_layer}: {e}"
        ) from e


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
        from data_engineering.core.sanity_check.utils import EnvironmentConfig

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


def validation_results(
    df_results: DataFrame,
    layer: str = DEFAULT_LAYER,
    **kwargs,
) -> None:
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


def _load_and_parse_io_config(
    io_config_path: str, node_name: str, layer: str
) -> Optional[Tuple[dict, str]]:
    """
    Load and parse io_config.yml for a node.

    Args:
        io_config_path: Path to the io_config.yml file
        node_name: Name of the node
        layer: Data layer

    Returns:
        Tuple containing flow configuration and io_key, or None if not found

    """
    io_config = ConfigLoader.load_yaml(
        io_config_path, "io_config.yml", p_fallback_value={}
    )
    if not io_config:
        return None

    domain, flow_name = node_name.split(".", 1)
    io_key = (
        flow_name.replace(FLOW_NAME_SUFFIX, "")
        if flow_name.endswith(FLOW_NAME_SUFFIX)
        else flow_name
    )
    flow_config = io_config.get(io_key) or {}

    if flow_config and isinstance(flow_config, dict) and "Input" in flow_config:
        return flow_config, io_key
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
    if cache_key in _DEPENDENCY_CACHE:
        return _DEPENDENCY_CACHE[cache_key]

    if "." not in node_name:
        _DEPENDENCY_CACHE[cache_key] = None
        return None

    try:
        from data_engineering.core.sanity_check.utils import EnvironmentConfig

        base_dir = EnvironmentConfig.get_root_dir()
        io_config_path = os.path.join(base_dir, layer, "conf", "io_config.yml")

        config_result = _load_and_parse_io_config(io_config_path, node_name, layer)
        if config_result is None:
            _DEPENDENCY_CACHE[cache_key] = None
            return None

        flow_config, io_key = config_result
        actual_datasets = _resolve_abstract_inputs_to_datasets(
            flow_config["Input"], layer, base_dir
        )
        actual_datasets = _add_output_datasets(
            layer, flow_config, actual_datasets, node_name
        )

        _DEPENDENCY_CACHE[cache_key] = actual_datasets
        return actual_datasets
    except Exception:
        _DEPENDENCY_CACHE[cache_key] = None
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
    if not data_catalog:
        logger.warning("Using abstract names as-is (no data_catalog.yml)")
        return set(abstract_inputs)

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

    _load_config = LayerUtils.create_layer_config_loader(layer_name)
    _validate_dataset = LayerUtils.create_layer_validator(layer_name)

    def _get_filtered_datasets(config_all: Dict[str, Any], context) -> list:
        """Get filtered list of datasets based on execution context."""
        configured_datasets = list(config_all.keys())

        if not (context.is_isolated_execution and os.getenv("node")):
            return configured_datasets

        mapping_layer = (
            dependency_mapping.get(os.getenv("workflow"), layer_name)
            if dependency_mapping
            else layer_name
        )

        if layer_name in ("data_integration", "data_transformation"):
            return configured_datasets

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
        start_dt: str,
        end_dt: str,
    ) -> Tuple[Optional[DataFrame], Dict]:
        """Orchestrate validation of a single dataset.

        Args:
            meta: Metadata dictionary for the dataset.
            config_all: Full configuration dictionary for the layer.
            start_dt: Start date for validation.
            end_dt: End date for validation.
        """

        result_df, summary = LayerUtils.validate_single_dataset(
            meta,
            config_all,
            loader_func,
            _validate_dataset,
            LayerUtils.log_summary,
            start_dt,
            end_dt,
        )

        summary = summary or {"decision": "SKIP"}

        return result_df, summary

    def _run_dataset_validation(
        datasets_metadata: List[Dict[str, Any]],
        config_all: Dict[str, Any],
        p_start_dt: str,
        p_end_dt: str,
    ) -> Tuple[Optional[DataFrame], List[Dict]]:
        """Run dataset validation, choosing strategy based on month range size.

        Args:
            datasets_metadata: List of metadata for all datasets.
            config_all: Full configuration dictionary.
            p_start_dt: Validation start date.
            p_end_dt: Validation end date.

        Returns:
            Tuple of (results_dataframe, list_of_critical_failures).
        """
        month_ranges = LayerUtils._resolve_month_ranges(p_start_dt, p_end_dt)

        if len(month_ranges) > _PRE_SAVE_MONTH_THRESHOLD:
            return LayerUtils.process_all_datasets_with_cache(
                datasets_metadata,
                config_all,
                loader_func,
                _validate_dataset,
                p_start_dt,
                p_end_dt,
            )
        else:
            return LayerUtils.process_all_datasets(
                datasets_metadata,
                config_all,
                process_dataset_task,
                p_start_dt,
                p_end_dt,
            )

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
            config_all = _load_config(p_domain_filter_override=context.domain)
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

            df_results, critical_fails = _run_dataset_validation(
                datasets_metadata, config_all, p_start_dt, p_end_dt
            )

            validation_results(df_results, layer=layer_name)

            if critical_fails:
                failed_datasets = [
                    s.get("dataset_name", "unknown") for s in critical_fails
                ]
                raise RuntimeError(
                    f"Critical failures in {layer_name} datasets: "
                    f"{', '.join(failed_datasets)}"
                )

        except Exception as e:
            logger.error(f"❌ Sanity check flow failed for {layer_name}: {e}")
            raise

    return sanity_check_flow
