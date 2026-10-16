"""Tests for the month-grouped engine: record assembly, blocks and persistence.

Spark is replaced by fakes: the aggregate and keyed jobs are patched to return
per-month rows, so these tests cover everything the engine does with them.
"""
from unittest.mock import MagicMock

import pytest

from data_engineering.core.sanity_check import grouped
from data_engineering.core.sanity_check.base import (
    QualitySeverity,
    ValidationStatus,
)
from data_engineering.core.sanity_check.grouped import (
    ROWS_ALIAS,
    GroupedValidationRunner,
    TrackingSink,
    emit_records_per_month,
    resolve_batch_months,
    split_into_blocks,
    summarize_month,
)
from data_engineering.core.sanity_check.standard_validators import (
    StandardConsistencyValidator,
    StandardDuplicatesValidator,
    StandardNullValuesValidator,
)
from data_engineering.core.sanity_check.utils import create_record

# --------------------------------------------------------------------------- #
# Pure helpers
# --------------------------------------------------------------------------- #


def test_resolve_batch_months(monkeypatch):
    monkeypatch.delenv(grouped.ENV_BATCH_MONTHS, raising=False)
    assert resolve_batch_months({}) == 12
    assert resolve_batch_months({"batch_months": 6}) == 6
    monkeypatch.setenv(grouped.ENV_BATCH_MONTHS, "3")
    assert resolve_batch_months({}) == 3
    assert resolve_batch_months({"batch_months": 24}) == 24
    assert resolve_batch_months({"batch_months": 0}) == 12
    assert resolve_batch_months({"batch_months": "x"}) == 12


def test_split_into_blocks():
    months = [(f"m{i}", f"e{i}") for i in range(5)]
    assert split_into_blocks(months, 2) == [months[0:2], months[2:4], months[4:5]]
    assert split_into_blocks([], 12) == []


def _record(category="NULL_VALUES", status=ValidationStatus.FAIL, severity="CRITICAL"):
    return create_record(
        p_dataset_name="ds",
        p_check_name="c",
        p_category=category,
        p_severity=QualitySeverity(severity),
        p_description="d",
        p_status=status,
    )


def test_summarize_month_decisions():
    critical = summarize_month("ds", "2024-01-31", [_record()], 1, True)
    assert critical["decision"] == "STOP" and critical["critical_failures"] == 1

    high = summarize_month("ds", "2024-01-31", [_record(severity="HIGH")], 1, True)
    assert high["decision"] == "PASS" and high["failures"] == 1

    error = _record(category="VALIDATOR_ERROR", severity="HIGH")
    assert summarize_month("ds", "m", [error], 1, True)["decision"] == "STOP"
    assert summarize_month("ds", "m", [error], 1, False)["decision"] == "PASS"


def test_emit_records_per_month_stamps_each_month():
    written = []
    months = [("2024-01-01", "2024-01-31"), ("2024-02-01", "2024-02-29")]
    summaries = emit_records_per_month("ds", [_record()], months, 1, written.append)

    assert [len(block) for block in written] == [1, 1]
    assert written[1][0]._observ_start_dt == "2024-02-01"
    assert written[1][0]._observ_end_dt == "2024-02-29"
    assert [s["decision"] for s in summaries] == ["STOP", "STOP"]


def test_tracking_sink_records_months_and_failures():
    sink = TrackingSink(lambda records: None)
    sink([grouped.stamp_records([_record()], "2024-01-01", "2024-01-31")[0]])
    assert sink.calls == 1 and sink.written_months == {"2024-01-31"}

    failing = TrackingSink(MagicMock(side_effect=IOError("disk")))
    with pytest.raises(IOError):
        failing([_record()])
    assert failing.failed


# --------------------------------------------------------------------------- #
# Validators: shared record building
# --------------------------------------------------------------------------- #


class _FakeFrame:
    """Just enough of a DataFrame for column resolution and filters."""

    def __init__(self, columns):
        self.columns = list(columns)

    def withColumn(self, *_):
        return self

    def filter(self, *_):
        return self

    def select(self, *_):
        return self

    @property
    def schema(self):
        raise RuntimeError("no schema in fake")


def test_null_values_records_match_thresholds():
    validator = StandardNullValuesValidator(
        {"columns": ["A"], "null_thresholds": {"b": 10}, "severity": "CRITICAL"},
        MagicMock(),
    )
    exprs, static = validator.prepare_aggregations(_FakeFrame(["a", "b"]), "ds", "p_")
    assert static == [] and len(exprs) == 3

    (alias_a, col_a, _), (alias_b, col_b, _) = validator._checks
    records = validator.build_records(
        {validator._rows_alias: 100, alias_a: 1, alias_b: 10}, "ds"
    )
    by_column = {r.columna: r for r in records}
    assert by_column["a"].estado == "FAIL" and by_column["a"].severidad == "CRITICAL"
    assert by_column["b"].estado == "PASS"  # 10% is within a 10% threshold


def test_missing_column_is_a_static_failure():
    validator = StandardNullValuesValidator(
        {"columns": ["missing"], "severity": "HIGH"}, MagicMock()
    )
    exprs, static = validator.prepare_aggregations(_FakeFrame(["a"]), "ds", "p_")
    assert exprs == []
    assert static[0].validacion == "MISSING_COLUMN" and static[0].severidad == "HIGH"


def test_duplicates_keyed_records():
    validator = StandardDuplicatesValidator({"primary_key": ["ID"]}, MagicMock())
    keys, static = validator.prepare_keys(_FakeFrame(["id", "x"]), "ds")
    assert keys == ["id"] and static == []

    failed = validator.build_keyed_records(
        {"dup_keys": 2, "dup_rows": 5, "null_key_rows": 3}, "ds"
    )[0]
    assert failed.estado == "FAIL" and failed.severidad == "HIGH"
    assert "2 valores de llave duplicados (5 registros afectados)" in failed.descripcion
    assert "3 registros con llave nula" in failed.descripcion

    passed = validator.build_keyed_records({"dup_keys": 0}, "ds")[0]
    assert passed.estado == "PASS"


def test_consistency_invalid_rule_fails_alone():
    validator = StandardConsistencyValidator(
        {"rules": [{"name": "bad", "condition": "x >", "severity": "CRITICAL"}]},
        MagicMock(),
    )
    exprs, static = validator.prepare_aggregations(_FakeFrame(["x"]), "ds", "p_")
    assert exprs == []
    assert static[0].columna == "bad" and static[0].severidad == "CRITICAL"


# --------------------------------------------------------------------------- #
# Runner: months, blocks, empty months, null dates, persistence
# --------------------------------------------------------------------------- #

CONFIG = {
    "batch_months": 2,
    "null_values": {"enabled": True, "columns": ["a"], "severity": "CRITICAL"},
    "duplicates": {"enabled": True, "primary_key": ["id"], "severity": "HIGH"},
}

# Rows and nulls of column "a" per month; None is the null-date group.
DATA = {
    "2024-01-31": {"rows": 10, "nulls": 0, "dups": 0},
    "2024-03-31": {"rows": 8, "nulls": 2, "dups": 1},
    None: {"rows": 4, "nulls": 0, "dups": 0},
}


@pytest.fixture
def runner(monkeypatch):
    monkeypatch.delenv(grouped.ENV_BATCH_MONTHS, raising=False)
    keyed_calls = []

    def fake_aggregate(self, df_m, groups, dataset_name):
        rows = {}
        for month, data in DATA.items():
            values = {ROWS_ALIAS: data["rows"]}
            for validator, _ in groups:
                values[validator._rows_alias] = data["rows"]
                for alias, _, _ in validator._checks:
                    values[alias] = data["nulls"]
            rows[month] = values
        return rows, []

    def fake_run_keyed(df_part, keyed, dataset_name, severity, label):
        keyed_calls.append(label)
        results = {}
        for month, data in DATA.items():
            for validator, _ in keyed:
                results.setdefault(month, []).extend(
                    validator.build_keyed_records(
                        {"dup_keys": data["dups"], "dup_rows": 2 * data["dups"]},
                        dataset_name,
                    )
                )
        return results

    monkeypatch.setattr(GroupedValidationRunner, "_aggregate", fake_aggregate)
    monkeypatch.setattr(
        GroupedValidationRunner, "_run_keyed", staticmethod(fake_run_keyed)
    )
    instance = GroupedValidationRunner("data_cleaning")
    instance.keyed_calls = keyed_calls
    return instance


MONTHS = [
    ("2024-01-01", "2024-01-31"),
    ("2024-02-01", "2024-02-29"),
    ("2024-03-01", "2024-03-31"),
]


def _by_month(blocks):
    out = {}
    for block in blocks:
        for record in block:
            out.setdefault(record._observ_end_dt, []).append(record)
    return out


def test_runner_writes_one_block_at_a_time(runner):
    written = []
    summaries = runner.run(
        _FakeFrame(["id", "a", "fecha"]),
        "ds",
        dict(CONFIG),
        "fecha",
        MONTHS,
        written.append,
        p_null_month_label="2024-03-31",
    )

    # 3 months in blocks of 2 → 2 writes; one duplicates job per block plus
    # one for the null-date rows
    assert len(written) == 2
    assert len(runner.keyed_calls) == 3
    assert runner.keyed_calls[0] == "ds SIN_FECHA"

    months = _by_month(written)
    assert {r.validacion for r in months["2024-01-31"]} == {"NULL_VALUES", "DUPLICATES"}
    assert all(r.estado == "PASS" for r in months["2024-01-31"])

    # February has no rows: only the EMPTY_DATASET record
    assert [r.validacion for r in months["2024-02-29"]] == ["EMPTY_DATASET"]
    assert months["2024-02-29"][0]._observ_start_dt == "2024-02-01"

    # March: CRITICAL nulls → STOP; null-date rows share March's write
    decisions = {s["month"]: s["decision"] for s in summaries}
    assert decisions == {
        "2024-01-31": "PASS",
        "2024-02-29": "PASS",
        "2024-03-31": "STOP",
        "SIN_FECHA": "PASS",
    }
    assert len([r for r in written[1] if r.validacion == "NULL_VALUES"]) == 2


def test_runner_derives_months_from_data(runner):
    written = []
    summaries = runner.run(
        _FakeFrame(["id", "a", "fecha"]),
        "ds",
        dict(CONFIG),
        "fecha",
        None,
        written.append,
        p_null_month_label="2024-05-15",
    )
    reported = [s["month"] for s in summaries]
    assert reported[:3] == ["2024-01-31", "2024-02-29", "2024-03-31"]
    # Null-date rows stamped with the label month, outside every block → last
    assert written[-1][-1]._observ_end_dt == "2024-05-15"


def test_runner_disabled_dataset_writes_skipped_per_month(runner):
    written = []
    summaries = runner.run(
        _FakeFrame(["id"]),
        "ds",
        {"enabled": False, **CONFIG},
        "fecha",
        MONTHS,
        written.append,
    )
    assert [r.estado for block in written for r in block] == ["SKIPPED"] * 3
    assert all(s["decision"] == "PASS" for s in summaries)
