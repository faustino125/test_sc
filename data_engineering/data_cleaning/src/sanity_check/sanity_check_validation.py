from data_engineering.core.sanity_check.sanity_check_flow import (
    create_sanity_check_flow,
)

sanity_check_flow = create_sanity_check_flow(
    layer_name="data_cleaning",
    tag_name="cleaning",
    dependency_mapping={
        "data_transformation": "data_transformation",
    },
)
