from prefect import flow

from data_engineering.core.dashboard_data_flow import orchestrate_qa_volume_task
from data_engineering.core.save_data_flow import save_data_flow


@flow(name="dashboard_volume_flow")
def dashboard_volume_flow():
    """
    The flow performs the following operations:
    1. Validates the data catalog and creates volume metrics.
    2. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value
    """
    df_volume = orchestrate_qa_volume_task(["id_cliente"], "2018-01-31")
    save_data_flow(df_volume)
