from data_engineering.core.sanity_check.engine import LayerUtils
from data_engineering.core.sanity_check.sanity_check_flow import (
    create_sanity_check_flow,
)

sanity_check_flow = create_sanity_check_flow(
    layer_name="data_transformation",
    loader_func=LayerUtils.load_dataset,
    tag_name="transformation",
    db_suffix="data_transformation",
    dependency_mapping={
        "data_integration": "data_integration",
        "data_cleaning": "data_cleaning",
    },
    use_catalog=False,
)
