from data_engineering.core.sanity_check.sanity_check_flow import (
    create_sanity_check_flow,
)

sanity_check_flow = create_sanity_check_flow(
    layer_name="data_transformation",
    tag_name="transformation",
    dependency_mapping={
        "data_integration": "data_integration",
        "data_cleaning": "data_cleaning",
    },
)
