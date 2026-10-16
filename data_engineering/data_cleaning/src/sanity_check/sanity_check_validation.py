from data_engineering.core.sanity_check.engine import LayerUtils
from data_engineering.core.sanity_check.sanity_check_flow import (
    create_sanity_check_flow,
)

sanity_check_flow = create_sanity_check_flow(
    layer_name="data_cleaning",
    loader_func=LayerUtils.load_dataset,
    tag_name="cleaning",
    db_suffix="data_cleaning",
    dependency_mapping={
        "data_transformation": "data_transformation",
    },
    use_catalog=False,
)
