from unittest.mock import MagicMock

import pytest

from data_engineering.core.sanity_check import engine
from data_engineering.core.sanity_check import sanity_check_flow as sc_flow
from data_engineering.core.sanity_check.base import BaseValidator, QualitySeverity
from data_engineering.core.sanity_check.engine import LayerUtils, ValidatorRegistry
from data_engineering.core.sanity_check.standard_validators import (
    StandardNullValuesValidator,
)

# --------------------------------------------------------------------------- #
# Month ranges
# --------------------------------------------------------------------------- #


def test_month_ranges_span_years():
    assert LayerUtils._resolve_month_ranges("2024-11-15", "2025-02-03") == [
        ("2024-11-01", "2024-11-30"),
        ("2024-12-01", "2024-12-31"),
        ("2025-01-01", "2025-01-31"),
        ("2025-02-01", "2025-02-28"),
    ]


def test_month_ranges_reject_inverted_range():
    with pytest.raises(ValueError, match="after"):
        LayerUtils._resolve_month_ranges("2025-03-01", "2025-01-31")


def test_month_ranges_reject_malformed_date():
    with pytest.raises(ValueError):
        LayerUtils._resolve_month_ranges("2025-13-01", "2025-12-31")


def test_month_ranges_default_to_previous_month_when_incomplete():
    ranges = LayerUtils._resolve_month_ranges("2025-01-01", None)
    assert len(ranges) == 1
    start, end = ranges[0]
    assert start.endswith("-01") and start[:7] == end[:7]


# --------------------------------------------------------------------------- #
# Column pruning
# --------------------------------------------------------------------------- #

COLUMNS = ["ID", "amount", "status", "fecha", "unused_a", "unused_b"]


def test_pruning_ignores_string_literals_and_functions():
    config = {
        "consistency": {
            "enabled": True,
            "rules": [
                {"condition": "status IN ('active', 'closed') AND abs(amount) > 0"},
                {"condition": "upper(status) <> \"x\" AND fecha >= DATE '2024-01-01'"},
            ],
        },
        "duplicates": {"enabled": True, "primary_key": ["id"]},
    }
    assert LayerUtils._extract_columns_for_caching(config, "fecha", COLUMNS) == [
        "ID",
        "amount",
        "fecha",
        "status",
    ]


def test_pruning_ignores_digits_inside_numbers():
    config = {"consistency": {"enabled": True, "rules": [{"condition": "amount < 1e5"}]}}
    assert LayerUtils._extract_columns_for_caching(config, None, COLUMNS) == ["amount"]


def test_pruning_skipped_for_unknown_identifier():
    config = {"consistency": {"enabled": True, "rules": [{"condition": "missing > 0"}]}}
    assert LayerUtils._extract_columns_for_caching(config, None, COLUMNS) == COLUMNS


def test_pruning_skipped_for_layer_specific_validator():
    config = {
        "duplicates": {"enabled": True, "primary_key": ["id"]},
        "range_check": {"enabled": True, "column": "amount"},
    }
    assert LayerUtils._extract_columns_for_caching(config, None, COLUMNS) == COLUMNS


# --------------------------------------------------------------------------- #
# Config matching and error classification
# --------------------------------------------------------------------------- #


def test_dataset_config_exact_and_case_insensitive():
    configs = {"Cli_Cliente": {"a": 1}}
    assert LayerUtils.get_dataset_config(configs, "Cli_Cliente") == {"a": 1}
    assert LayerUtils.get_dataset_config(configs, "cli_cliente") == {"a": 1}


def test_dataset_config_ambiguous_partial_match_returns_none():
    configs = {"account": {"a": 1}, "cc_account_x": {"b": 2}}
    assert LayerUtils.get_dataset_config(configs, "cc_account") is None


@pytest.mark.parametrize(
    "message, expected",
    [
        ("[TABLE_OR_VIEW_NOT_FOUND] The table or view `x` cannot be found.", True),
        ("[DELTA_TABLE_NOT_FOUND] Delta table `x` doesn't exist.", True),
        ("[SCHEMA_NOT_FOUND] The schema `dev_x` cannot be found.", False),
        ("Catalog 'bi_dev_de' does not exist", False),
        ("PERMISSION_DENIED: User does not have SELECT", False),
    ],
)
def test_missing_table_error_is_narrow(message, expected):
    assert LayerUtils._is_missing_table_error(message) is expected


@pytest.mark.parametrize(
    "message, expected",
    [
        ("Quality Gate FAILED: x has CRITICAL failures", True),
        ("Data persistence blocked: pre-save sanity check failed for 'x'", True),
        ("Blocked by quality gate: x 2025-01-01 → 2025-01-31", True),
        ("❌ SANITY CHECK failed for raw_data: boom", False),
        ("Some other error", False),
    ],
)
def test_is_quality_gate_error(message, expected):
    assert sc_flow.is_quality_gate_error(message) is expected


# --------------------------------------------------------------------------- #
# Severity and thresholds
# --------------------------------------------------------------------------- #


def test_coerce_severity():
    assert BaseValidator.coerce_severity(" critical ") is QualitySeverity.CRITICAL
    assert BaseValidator.coerce_severity(None) is QualitySeverity.MEDIUM
    with pytest.raises(ValueError):
        BaseValidator.coerce_severity("URGENT")


def test_null_threshold_coercion():
    assert StandardNullValuesValidator._coerce_threshold("c", "5") == 5.0
    with pytest.raises(ValueError):
        StandardNullValuesValidator._coerce_threshold("c", "five")
    with pytest.raises(ValueError):
        StandardNullValuesValidator._coerce_threshold("c", 150)


# --------------------------------------------------------------------------- #
# Validator registry
# --------------------------------------------------------------------------- #


def test_standard_validators_registered_for_every_layer():
    registry = ValidatorRegistry()
    engine._initialize_layer_validators(registry)
    for layer in LayerUtils.VALID_LAYERS:
        for validator_type in ("duplicates", "null_values", "dimensional", "consistency"):
            assert registry.get_validator_class(validator_type, layer) is not None


# --------------------------------------------------------------------------- #
# Session retry
# --------------------------------------------------------------------------- #


def test_session_retry_resets_session_and_retries(monkeypatch):
    reset = MagicMock()
    monkeypatch.setattr(engine, "reset_spark", reset)
    action = MagicMock(side_effect=[Exception("INVALID_HANDLE.SESSION_NOT_FOUND"), 7])

    assert LayerUtils._with_session_retry(action, "x") == 7
    reset.assert_called_once()


def test_session_retry_raises_non_session_error(monkeypatch):
    monkeypatch.setattr(engine, "reset_spark", MagicMock())
    action = MagicMock(side_effect=Exception("PERMISSION_DENIED"))

    with pytest.raises(Exception, match="PERMISSION_DENIED"):
        LayerUtils._with_session_retry(action, "x")
    assert action.call_count == 1


# --------------------------------------------------------------------------- #
# Quality gate (fail closed)
# --------------------------------------------------------------------------- #


@pytest.fixture
def gate_env(monkeypatch):
    """Isolate check_quality_gate from config files and the real session."""
    monkeypatch.setenv("env", "dev")
    monkeypatch.delenv("output_domain", raising=False)
    monkeypatch.setattr(
        engine.EnvironmentConfig, "get_catalog_name", classmethod(lambda cls: "bi_dev_de")
    )
    monkeypatch.setattr(engine, "load_yaml_file", lambda path: {})
    spark = MagicMock()
    monkeypatch.setattr(engine, "get_spark", lambda: spark)
    return spark


def _gate(dataset="tbl", start="2025-01-01", end="2025-01-31"):
    return LayerUtils.check_quality_gate(dataset, start, end, p_layer="data_cleaning")


def test_gate_blocks_on_invalid_dates(gate_env):
    assert _gate(start="2025/01/01") is False


def test_gate_blocks_on_empty_dataset_name(gate_env):
    assert _gate(dataset="") is False


def test_gate_blocks_when_table_lookup_fails(gate_env):
    gate_env.catalog.tableExists.side_effect = Exception("PERMISSION_DENIED")
    assert _gate() is False


def test_gate_passes_when_history_table_absent(gate_env):
    gate_env.catalog.tableExists.return_value = False
    assert _gate() is True


def test_gate_blocks_on_critical_history(gate_env):
    gate_env.catalog.tableExists.return_value = True
    gate_env.sql.return_value.count.return_value = 2
    assert _gate() is False


def test_gate_passes_without_critical_history(gate_env):
    gate_env.catalog.tableExists.return_value = True
    gate_env.sql.return_value.count.return_value = 0
    assert _gate() is True


def test_gate_blocks_when_query_fails(gate_env):
    gate_env.catalog.tableExists.return_value = True
    gate_env.sql.side_effect = Exception("[SCHEMA_NOT_FOUND] does not exist")
    assert _gate() is False


# --------------------------------------------------------------------------- #
# execute_flows surfaces quality gate blocks
# --------------------------------------------------------------------------- #


def test_execute_flows_runs_all_nodes_then_fails_on_blocked(monkeypatch):
    from data_engineering.core import utils as core_utils

    executed = []

    def fake_execute_flow(module, flow_name, flow):
        executed.append(flow_name)
        if flow_name == "b_flow":
            raise RuntimeError("Data persistence blocked: pre-save sanity check failed")

    monkeypatch.setattr(core_utils, "import_flow_module", lambda flow: MagicMock())
    monkeypatch.setattr(core_utils, "execute_flow", fake_execute_flow)
    monkeypatch.setattr(core_utils, "send_message_teams", MagicMock())
    monkeypatch.setenv("env", "local")

    flows = [f"data_engineering.data_cleaning.src.dom.{n}" for n in ("a_flow", "b_flow", "c_flow")]
    with pytest.raises(RuntimeError, match="b_flow"):
        core_utils.execute_flows(flows, "main_flow", p_sanity_check=True, workflow="data_cleaning")

    assert executed == ["a_flow", "b_flow", "c_flow"]


# --------------------------------------------------------------------------- #
# Results persistence: compact DELETE predicate
# --------------------------------------------------------------------------- #


def test_merge_delete_condition_groups_months_by_dataset():
    from data_engineering.core.save_data_flow import _build_merge_delete_condition

    combos = [
        {"nombre_dataset": "b", "_observ_end_dt": "2024-02-29"},
        {"nombre_dataset": "a", "_observ_end_dt": "2024-01-31"},
        {"nombre_dataset": "a", "_observ_end_dt": "2024-02-29"},
        {"nombre_dataset": "o'x", "_observ_end_dt": None},
    ]
    assert _build_merge_delete_condition(combos) == (
        "(nombre_dataset = 'a' AND (_observ_end_dt IN ('2024-01-31', '2024-02-29')))"
        " OR (nombre_dataset = 'b' AND (_observ_end_dt IN ('2024-02-29')))"
        " OR (nombre_dataset = 'o''x' AND (_observ_end_dt IS NULL))"
    )
    assert _build_merge_delete_condition([]) == ""


# --------------------------------------------------------------------------- #
# Config loading, registry fallback and result dates
# --------------------------------------------------------------------------- #


def _write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)


def test_load_config_by_domain_file_and_subdirectory(tmp_path):
    src = tmp_path / "data_cleaning" / "src"
    _write(src / "bel_sanity_check.yml", "a: {x: 1}\n")
    _write(src / "cc" / "cc_sanity_check.yml", "b: {y: 2}\n")
    _write(src / "_hidden" / "h_sanity_check.yml", "h: {z: 3}\n")

    assert LayerUtils.load_config(str(src), "bel") == {"a": {"x": 1}}
    assert LayerUtils.load_config(str(src), "cc") == {"b": {"y": 2}}
    assert LayerUtils.load_config(str(src), "missing") == {}
    # Without a filter, top-level files win over subdirectories
    assert LayerUtils.load_config(str(src)) == {"a": {"x": 1}}


def test_load_config_scans_subdirectories_without_top_level_files(tmp_path):
    src = tmp_path / "data_cleaning" / "src"
    _write(src / "cc" / "cc_sanity_check.yml", "b: {y: 2}\n")
    _write(src / "_hidden" / "h_sanity_check.yml", "h: {z: 3}\n")
    _write(src / "conf" / "c_sanity_check.yml", "c: {w: 4}\n")

    assert LayerUtils.load_config(str(src)) == {"b": {"y": 2}}


def test_registry_falls_back_to_common_validators():
    registry = ValidatorRegistry()
    registry.register_validator("common", int)
    registry.register_validator("common", str, p_layer="raw_data")

    assert registry.get_validator_class("common", "raw_data") is str
    assert registry.get_validator_class("common", "data_cleaning") is int
    assert registry.get_validator_class("unknown", "raw_data") is None


def test_to_dataframe_keeps_record_dates_and_fills_defaults(monkeypatch):
    from datetime import date

    from data_engineering.core.sanity_check.utils import create_record, to_dataframe

    monkeypatch.setenv("end_dt", "2024-03-31")
    spark = MagicMock()
    stamped = create_record("ds", "c", "X", QualitySeverity.LOW, "d")
    stamped._observ_start_dt, stamped._observ_end_dt = "2024-01-01", "2024-01-31"
    as_date = create_record("ds", "c", "X", QualitySeverity.LOW, "d")
    as_date._observ_start_dt = date(2024, 2, 1)
    bare = create_record("ds", "c", "X", QualitySeverity.LOW, "d")

    to_dataframe(spark, [stamped, as_date, bare])
    rows = spark.createDataFrame.call_args[0][0]

    assert [r["_observ_start_dt"] for r in rows] == [
        "2024-01-01",
        "2024-02-01",
        "2024-03-01",
    ]
    assert [r["_observ_end_dt"] for r in rows] == [
        "2024-01-31",
        "2024-03-31",
        "2024-03-31",
    ]
