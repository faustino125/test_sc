from data_engineering.core.sanity_check.sanity_check_flow import (
    create_sanity_check_flow,
)

# Datasets not found in data_cleaning's catalog fall back to <env>_raw_data tables.
sanity_check_flow = create_sanity_check_flow(
    layer_name="raw_data",
    tag_name="raw_data",
    dependency_mapping={"data_cleaning": "data_cleaning"},
    use_catalog=True,
)
