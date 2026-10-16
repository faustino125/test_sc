from prefect import flow, task
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns


@arrange_columns(p_start_cols=["_observ_end_dt", "_particion", "id_cliente"])
@task(name="transform_joint_account_task", tags=["data transformation", "processing"])
def transform_joint_account_task(p_joint_account) -> DataFrame:
    """Transforms joint account data by selecting and reorganizing key columns.

    Args:
        p_joint_account (DataFrame): Source DataFrame containing cleaned joint account.

    Returns:
        DataFrame: Transformed data with selected columns in specified order:
            - First columns: _observ_end_dt, _particion, id_cliente
    """
    df_joint_account_features = p_joint_account.select(
        f.col("fecha_informacion"),
        f.col("id_cliente"),
        f.col("cuenta_corporativa"),
        f.col("clase_cliente"),
        f.col("id_producto"),
        f.col("descripcion_producto"),
        f.col("saldo"),
        f.col("id_moneda"),
        f.col("porcentaje"),
        f.col("id_situacion_cuenta"),
        f.col("situacion_cuenta"),
        f.col("situacion_cuenta_homologado"),
        f.col("cliente_mancomunado_flag"),
        f.col("_observ_end_dt"),
    )
    return df_joint_account_features


@flow(name="joint_account_flow")
def joint_account_flow():
    """
    Loads, transforms and saves joint_account features in the data lake.

    The flow performs the following operations:
    1. Loads clean joint_account data using the specified date range.
    2. Transforms and processes the joint_account features data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed
        joint_account features data.
    """
    raw_data = load_raw_data_flow()

    df_joint_account_features = transform_joint_account_task(
        raw_data["cleaned_joint_account"]
    )

    save_data_flow(df_joint_account_features)
