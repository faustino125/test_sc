"""Test bootstrap: stub Spark/Databricks/Prefect when they are not installed.

The sanity check logic under test (date ranges, column extraction, config
matching, quality gate decisions) is plain Python; only module imports need
these packages, so lightweight stand-ins are enough to run it locally.
"""
import importlib.util
import os
import sys
import types
from unittest.mock import MagicMock

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)


def _passthrough_decorator(*args, **kwargs):
    """Stand-in for prefect's @flow/@task, bare or with arguments."""
    if len(args) == 1 and callable(args[0]) and not kwargs:
        return args[0]
    return lambda fn: fn


def _module(name, **attrs):
    module = types.ModuleType(name)
    module.__dict__.update(attrs)
    sys.modules[name] = module
    return module


class _BooleanType:
    def simpleString(self):
        return "boolean"


if importlib.util.find_spec("pyspark") is None:
    pyspark = _module("pyspark", StorageLevel=MagicMock())
    sql = _module(
        "pyspark.sql",
        Column=MagicMock,
        DataFrame=MagicMock,
        SparkSession=MagicMock,
        functions=MagicMock(),
    )
    _module("pyspark.sql.functions")
    types_mod = _module(
        "pyspark.sql.types",
        StringType=MagicMock,
        StructField=MagicMock,
        StructType=MagicMock,
        BooleanType=_BooleanType,
        DecimalType=MagicMock,
        DoubleType=MagicMock,
        FloatType=MagicMock,
    )
    _module("pyspark.storagelevel", StorageLevel=MagicMock())
    pyspark.sql = sql
    sql.types = types_mod


def _missing(name):
    try:
        return importlib.util.find_spec(name) is None
    except ModuleNotFoundError:
        return True


if _missing("databricks.connect"):
    connect = _module("databricks.connect", DatabricksSession=MagicMock())
    parent = (
        _module("databricks")
        if _missing("databricks")
        else importlib.import_module("databricks")
    )
    parent.connect = connect

if importlib.util.find_spec("prefect") is None:
    _module(
        "prefect",
        flow=_passthrough_decorator,
        task=_passthrough_decorator,
        get_run_logger=lambda: MagicMock(),
    )
