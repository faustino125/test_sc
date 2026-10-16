from prefect import flow

from data_engineering.core.check_date_integrity import (
    check_information_by_date_task,
    evaluate_missing_information_task,
    get_date_range_check_task,
)
from data_engineering.core.save_data_flow import save_data_flow


@flow(name="check_date_flow")
def check_date_flow():
    """
    The flow performs the following operations:
    1. Validates the number of records for each table.
    2. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value
    """
    custom_validations = {"eqpr": 11}
    error_message = ("Data Cleaning without month information",)
    start_date, end_date = get_date_range_check_task()
    df_check_date = check_information_by_date_task(
        start_date, end_date, ["cli_catalogo_informacion_financiera_juridico"]
    )
    save_data_flow(df_check_date)
    evaluate_missing_information_task(df_check_date, error_message, custom_validations)
