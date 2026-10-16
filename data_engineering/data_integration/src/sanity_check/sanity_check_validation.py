from data_engineering.core.sanity_check.engine import LayerUtils
from data_engineering.core.sanity_check.sanity_check_flow import (
    create_sanity_check_flow,
)

sanity_check_flow = create_sanity_check_flow(
    layer_name="data_integration",
    loader_func=LayerUtils.load_dataset,
    tag_name="integration",
    db_suffix="data_integration",
    use_catalog=False,
)
