import os
from typing import Any, Dict, List

from data_engineering.core.sanity_check.engine import LayerUtils
from data_engineering.core.sanity_check.sanity_check_flow import (
    create_sanity_check_flow,
)


def _create_missing_metadata(missing_names: set) -> List[Dict[str, Any]]:
    """Create metadata entries for datasets not found in catalog.

    Raw data layer specific: fallback when datasets aren't in data_cleaning's catalog.
    """
    env = os.getenv("env", "dev")
    return [
        {
            "dataset_name": name,
            "database": f"{env}_raw_data",
            "date_col": "_observ_end_dt",
        }
        for name in missing_names
    ]


sanity_check_flow = create_sanity_check_flow(
    layer_name="raw_data",
    loader_func=LayerUtils.load_dataset,
    tag_name="raw_data",
    db_suffix="raw_data",
    dependency_mapping={"data_cleaning": "data_cleaning"},
    use_catalog=True,
    create_missing_metadata_func=_create_missing_metadata,
)
