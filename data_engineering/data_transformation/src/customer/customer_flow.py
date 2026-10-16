from prefect import flow, task
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
from data_engineering.data_transformation.src.utils.utils import (
    fillna_flags_task,
    join_dataframes_task,
)


@task(name="process_customer_task", tags=["data transformation", "processing"])
def process_customer_task(
    p_cleaned_customer: DataFrame,
    p_cleaned_ire_data: DataFrame,
    p_cleaned_employee: DataFrame,
) -> DataFrame:
    """
    Transforms and processes customer data.

    This task processes the clean customer, legal client, and employee data by applying
    transformations and transforming steps. It standardizes column names, creates new
    columns based on transformations, and joins them into a unified dataset.

    Args:
        p_cleaned_customer (DataFrame): Cleaned customer data.
        p_cleaned_ire_data (DataFrame): Cleaned legal client data.
        p_cleaned_employee (DataFrame): Cleaned employee data.

    Returns:
        DataFrame: Transformed customer DataFrame.
    """
    df_ire_customer_data = p_cleaned_ire_data.filter(f.col("fila") == 1)

    df_employee = (
        p_cleaned_employee.filter(f.col("flag_empleado_activo") == 1)
        .select(
            f.col("id_cliente"),
            f.col("_observ_end_dt"),
            f.col("flag_empleado_activo").alias("flag_empleado_corp_bi"),
        )
        .distinct()
    )

    df_customer = p_cleaned_customer.join(
        df_ire_customer_data, on=["id_cliente", "_observ_end_dt"], how="left"
    )

    df_customer_final = df_customer.join(
        df_employee, on=["id_cliente", "_observ_end_dt"], how="left"
    ).select(
        f.col("id_cliente"),
        f.col("carterizacion"),
        f.coalesce(f.col("antiguedad_cliente_dias"), f.lit(0)).alias(
            "antiguedad_cliente_dias"
        ),
        f.col("edad"),
        f.col("genero"),
        f.col("cuenta_planilla8"),
        f.col("estado_civil"),
        f.col("profesion"),
        f.col("flag_cliente_5x1"),
        f.col("id_tipo_cliente"),
        f.col("nacionalidad"),
        f.col("flag_mal_deudor"),
        f.coalesce(
            f.col("ire_fecha_constitucion_empresa"), f.col("fecha_constitucion_empresa")
        ).alias("fecha_constitucion_empresa"),
        f.coalesce(
            f.col("ire_antiguedad_empresa_dias"), f.col("antiguedad_empresa_dias")
        ).alias("dias_constitucion_empresa"),
        f.col("banca_favorita"),
        f.when(
            f.col("flag_empleado_corp_bi").isNotNull(), f.col("flag_empleado_corp_bi")
        )
        .otherwise(f.lit(0))
        .alias("flag_empleado_corp_bi"),
        f.col("flag_profesion_lista_negra"),
        f.col("flag_bimovil"),
        f.col("conteo_club_bi"),
        f.col("grupo_corporativo"),
        f.col("anio_informacion"),
        f.col("_observ_end_dt"),
    )

    return df_customer_final


@task(name="process_income_task", tags=["data transformation", "processing"])
def process_income_task(
    p_cleaned_income: DataFrame,
) -> DataFrame:
    """
    transforms and processes customer data.

    This task processes the clean customer income data by applying a sum aggregation
    so it can return the sum of the average income in a single column grouped by
    customer.

    Args:
        p_cleaned_income (DataFrame): Cleaned customer income data.

    Returns:
        DataFrame: Transformed customer income DataFrame.
    """

    df_income_final = p_cleaned_income.groupBy("id_cliente", "_observ_end_dt").agg(
        f.sum(f.col("monto_promedio")).cast("float").alias("ingreso_mensual")
    )
    return df_income_final


@task(name="process_main_address_task", tags=["data transformation", "processing"])
def process_main_address_task(
    p_cleaned_main_address: DataFrame,
) -> DataFrame:
    """
    Transforms and processes customer main address data.

    This task processes the clean customer, and main address data by applying
    transformations and transforming steps. It standardizes column names, creates new
    columns based on transformations, and joins them into a unified dataset.

    Args:
        p_cleaned_main_address (DataFrame): Cleaned customer main address data.

    Returns:
        DataFrame: Transformed customer with main address DataFrame.
    """

    df_final_customer = p_cleaned_main_address.select(
        f.col("id_cliente"),
        f.col("departamento_residencia"),
        f.col("municipio_residencia"),
        f.col("flag_zona_roja"),
    )
    return df_final_customer


@task(name="process_ire_task", tags=["data transformation", "processing"])
def process_ire_task(
    p_cleaned_ire: DataFrame,
) -> DataFrame:
    """
    transforms and processes the start of relationship form (IRE) data.

    This task processes the clean customer, and IRE data by applying
    transformations and transforming steps. It standardizes column names, creates new
    columns based on transformations, and joins them into a unified dataset.

    Args:
        p_cleaned_ire (DataFrame): Cleaned ire form data.

    Returns:
        DataFrame: Transformed customer ire DataFrame.
    """

    df_filtered_ire = p_cleaned_ire.filter(f.col("fila") == 1).select(
        f.col("id_cliente"),
        f.col("ingresos_mensuales").alias("ire_ingresos_mensuales"),
        f.col("egresos_mensuales").alias("ire_egresos_mensuales"),
        f.col("clase_actividad_economica").alias("ire_clase_actividad_economica"),
        f.col("tipo_actividad_economica").alias("ire_tipo_actividad_economica"),
        f.col("descrip_otra_act_economica").alias("ire_descrip_otra_act_economica"),
        f.col("cant_subagentes").alias("ire_total_subagente"),
        f.col("cant_empleados").alias("ire_total_empleados"),
        f.col("cpe_flag").alias("ire_flag_cpe"),
        f.col("fecha_nac_rl").alias("ire_fecha_nacimiento"),
        f.col("edad_rl").alias("ire_edad"),
        f.col("pep_rl_flag").alias("ire_flag_pep"),
        f.col("relacion_pep_rl_flag").alias("ire_flag_pariente_pep"),
        f.col("asociacion_pep_rl_flag").alias("ire_flag_asociacion_rl_pep"),
        f.col("_observ_end_dt"),
    )

    return df_filtered_ire


@task(name="process_bel_web_logins_task", tags=["data transformation", "processing"])
def process_bel_web_logins_task(
    p_cleaned_bel_login: DataFrame,
    p_cleaned_bel_users: DataFrame,
) -> DataFrame:
    """
    transforms and processes all bel web logins.

    This task processes the clean bel web logins data by applying
    transformations to it. It standardizes column names, creates new
    columns based on transformations, and joins them into a unified dataset.

    Args:
        p_cleaned_bel_login (DataFrame): Cleaned bel web logins data.
        p_cleaned_bel_users (DataFrame): Cleaned bel users data.

    Returns:
        DataFrame: Transformed customer bel web logins DataFrame.
    """
    df_bel_logins = p_cleaned_bel_login.join(
        p_cleaned_bel_users, on=["id_instalacion"]
    ).select(
        p_cleaned_bel_login["*"],
        f.col("id_cliente"),
        f.col("ultimo_inicio_sesion")
        .between(
            p_cleaned_bel_login["_observ_start_dt"],
            p_cleaned_bel_login["_observ_end_dt"],
        )
        .cast("int")
        .alias("inicio_de_sesion_flag"),
    )

    df_bel_logins = df_bel_logins.groupBy(
        f.col("id_cliente"), f.col("_observ_end_dt")
    ).agg(
        f.max("inicio_de_sesion_flag").alias("bel_web_login_mes_flag"),
        f.when(f.max("inicio_de_sesion_flag") > 0, f.max("ultimo_inicio_sesion")).alias(
            "bel_web_ultimo_login_mes_dt"
        ),
        f.lit(1).alias("tiene_bel_flag"),
    )

    return df_bel_logins


@task(name="process_bel_app_logins_task", tags=["data transformation", "processing"])
def process_bel_app_logins_task(
    p_cleaned_bel_app_login: DataFrame,
    p_cleaned_bel_users: DataFrame,
) -> DataFrame:
    """
    transforms and processes all bel app logins.

    This task processes the clean bel app logins data by applying
    transformations to it. It standardizes column names, creates new
    columns based on transformations, and joins them into a unified dataset.

    Args:
        p_cleaned_bel_app_login (DataFrame): Cleaned bel app login data.
        p_cleaned_bel_users (DataFrame): Cleaned bel users data.

    Returns:
        DataFrame: Transformed customer bel app logins DataFrame.
    """
    df_bel_app_login = p_cleaned_bel_app_login.filter(
        (f.col("descripcion_operacion").like("%login%"))
        | f.col("descripcion_operacion").like("%autenticar%")
    )

    df_bel_app_login = df_bel_app_login.join(
        p_cleaned_bel_users, on=["id_instalacion"]
    ).select(
        df_bel_app_login["*"],
        f.col("id_cliente"),
    )

    df_bel_app_login = df_bel_app_login.groupBy(
        f.col("id_cliente"), f.col("_observ_end_dt")
    ).agg(
        f.lit(1).alias("bel_app_login_mes_flag"),
        f.max("fecha_transaccion").alias("bel_app_ultimo_login_mes_dt"),
    )

    return df_bel_app_login


@task(name="process_bbk_supplier_task", tags=["data transformation", "processing"])
def process_bbk_supplier_task(
    p_cleaned_customer: DataFrame,
    p_cleaned_bbk_supplier: DataFrame,
) -> DataFrame:
    """
    Transforms BiBanking supplier data to create a supplier flag exclusively for
    legal clients.

    This task processes the clean BiBanking supplier and customer data by applying
    transformations. It takes in consideration only juridic clients and then
    creates a supplier flag only valid to this segment of clients, based on the
    `descripcion_operacion` field.

    Args:
        p_cleaned_bbk_supplier (DataFrame): Clean BiBanking supplier data.
        p_cleaned_customer (DataFrame): Cleaned customer data.

    Returns:
        DataFrame: Transformed  bbk customer DataFrame.
    """
    df_flag_supplier = p_cleaned_customer.join(
        p_cleaned_bbk_supplier,
        (p_cleaned_customer.id_cliente == p_cleaned_bbk_supplier.id_cliente_destino)
        & (p_cleaned_customer._observ_end_dt == p_cleaned_bbk_supplier._observ_end_dt),
    ).select(
        f.col("id_cliente_destino").alias("id_cliente"),
        f.when((f.col("descripcion_operacion")).isNull(), f.lit(0))
        .otherwise(1)
        .cast("int")
        .alias("flag_descripcion_operacion"),
        f.col("monto_credito_proveedor_flag"),
        p_cleaned_customer._observ_end_dt,
    )

    df_bbk_supplier = (
        df_flag_supplier.groupBy(["id_cliente", "_observ_end_dt"]).agg(
            f.max(f.col("flag_descripcion_operacion")).alias("bbk_flag_proveedor"),
            f.max(f.col("monto_credito_proveedor_flag")).alias(
                "monto_credito_proveedor_flag"
            ),
        )
    ).fillna(0)

    return df_bbk_supplier


@task(name="process_leads_task", tags=["data transformation", "processing"])
def process_leads_task(
    p_cleaned_leads: DataFrame,
) -> DataFrame:
    """
    Transforms leads data to create lead flags exclusively for
    legal clients.

    This task processes the clean leads and customer data by applying
    transformations.

    Args:
        p_cleaned_leads (DataFrame): Clean leads data.

    Returns:
        DataFrame: Transformed leads DataFrame.
    """

    df_leads = p_cleaned_leads.select(
        f.col("id_cliente"),
        f.col("id_motivo_exclusion"),
        f.col("flag_exclusion"),
        f.col("fecha_inicio_lead"),
        f.col("fecha_fin_lead"),
        f.col("banca_lead"),
        f.col("monto_pre_autorizado_lead"),
        f.col("canal_tradicional_lead"),
        f.col("tasa_cn_lead"),
        f.col("tasa_ss_lead"),
        f.col("plazo_lead"),
        f.col("contactable_lead"),
        f.col("score_credito_lead"),
        f.col("desembolso_belapp_disponible_lead"),
        f.col("_observ_end_dt"),
    )

    return df_leads


@task(
    name="process_customer_financing_task", tags=["data transformation", "processing"]
)
def process_customer_financing_summary_task(
    p_cleaned_cc_customer_extra_financing: DataFrame,
) -> DataFrame:
    """
    Returns financing information at the customer and general classification levels,
    regardless og the specific financing type

    Args:
        p_cleaned_cc_customer_financing (DataFrame): Clean customer financing


    Returns:
        DataFrame: Financing information
    """
    df_cc_customer_extra_financing = p_cleaned_cc_customer_extra_financing.select(
        f.col("id_cliente"),
        f.col("_observ_end_dt"),
        f.col("flag_elegible_neocuotas"),
        f.col("flag_elegible_extrafinanciamiento"),
        f.col("limite_neocuotas_gtq"),
        f.col("limite_extrafinanciamiento_gtq"),
        f.col("monto_disponible_neocuotas_gtq"),
        f.col("monto_disponible_extrafinanciamiento_gtq"),
        f.col("limite_cuota_mensual_neocuotas_gtq"),
        f.col("limite_cuota_mensual_extrafinanciamiento_gtq"),
        f.col("cuota_mensual_disponible_neocuotas_gtq"),
        f.col("cuota_mensual_disponible_extrafinanciamiento_gtq"),
    )

    exclude_columns = ["id_cliente", "_observ_end_dt"]
    prefix = "tc_"
    df_cc_customer_extra_financing = df_cc_customer_extra_financing.select(
        *[
            col if col in exclude_columns else f.col(col).alias(f"{prefix}{col}")
            for col in df_cc_customer_extra_financing.columns
        ]
    )

    return df_cc_customer_extra_financing


@task(name="process_legal_financial_task", tags=["data transformation", "processing"])
def process_legal_financial_task(
    p_cleaned_legal_financial: DataFrame,
) -> DataFrame:
    """
    Transforms legal financial data to create legal financial features.

    This task processes the clean legal financial data by applying
    transformations.

    Args:
        p_cleaned_legal_financial (DataFrame): Clean legal financial data.

    Returns:
        DataFrame: Transformed legal financial DataFrame.
    """
    df_legal_financial = p_cleaned_legal_financial.select(
        f.col("id_cliente"),
        f.col("ingresos_anuales_moodys_gtq"),
        f.col("ingresos_anuales_sib_gtq"),
        f.col("ingresos_anuales_e83_gtq"),
        f.col("ingresos_anuales_ire_gtq"),
        f.col("ingresos_anuales_monetarios_gtq"),
        f.col("anio_informacion"),
    )

    return df_legal_financial


@arrange_columns(p_start_cols=["_observ_end_dt", "id_cliente"])
@task(name="transform_customer_task", tags=["data transformation", "processing"])
def transform_customer_task(
    p_cleaned_customer: DataFrame,
    p_cleaned_employee: DataFrame,
    p_cleaned_income: DataFrame,
    p_cleaned_main_address: DataFrame,
    p_cleaned_ire: DataFrame,
    p_cleaned_bbk_supplier: DataFrame,
    p_cleaned_bel_login: DataFrame,
    p_cleaned_bel_users: DataFrame,
    p_cleaned_bel_app_master: DataFrame,
    p_cleaned_leads: DataFrame,
    p_cleaned_cc_customer_extra_financing: DataFrame,
    p_cleaned_legal_financial: DataFrame,
) -> DataFrame:
    """
    Transforms and processes customer information to create customer features.

    This task processes the clean customer, employee, products, income,
    main address, ire, and bbk supplier data by
    applying transformation steps. It creates new columns based on joins,
    creates new data based on transformations, and joins them into a
    unified dataset.

    Args:
        p_cleaned_customer (DataFrame): Cleaned customer data.
        p_cleaned_employee (DataFrame): Cleaned employee data.
        p_cleaned_income (DataFrame): Cleaned customer income data.
        p_cleaned_main_address (DataFrame): Cleaned customer main address data.
        p_cleaned_ire (DataFrame): Cleaned customer ire form data.
        p_cleaned_bbk_supplier (DataFrame): Cleaned Bi Banking supplier data.
        p_cleaned_bel_login (DataFrame): Cleaned bel web login data.
        p_cleaned_bel_users (DataFrame): Cleaned bel users data.
        p_cleaned_bel_app_master (DataFrame): Cleaned bel app login data.
        p_cleaned_leads (DataFrame): Cleaned leads data.
        p_cleaned_cc_customer_extra_financing (DataFrame): Customer financing.
        p_cleaned_legal_financial (DataFrame): Cleaned legal financial.

    Returns:
        DataFrame: Customer features DataFrame.
    """

    df_legal_cliente = p_cleaned_customer.filter(f.col("id_tipo_cliente") == 2)
    df_base_customer = process_customer_task(
        p_cleaned_customer, p_cleaned_ire, p_cleaned_employee
    )
    df_income = process_income_task(p_cleaned_income)
    df_main_address = process_main_address_task(p_cleaned_main_address)
    df_ire = process_ire_task(p_cleaned_ire)
    df_bbk_supplier = process_bbk_supplier_task(
        df_legal_cliente, p_cleaned_bbk_supplier
    )
    df_bel_web_login = process_bel_web_logins_task(
        p_cleaned_bel_login, p_cleaned_bel_users
    )

    df_bel_app_login = process_bel_app_logins_task(
        p_cleaned_bel_app_master, p_cleaned_bel_users
    )

    df_leads = process_leads_task(p_cleaned_leads)

    df_base_customer = df_base_customer.join(
        df_main_address, ["id_cliente"], how="left"
    )

    df_legal_financial = process_legal_financial_task(p_cleaned_legal_financial)
    df_base_customer = df_base_customer.join(
        df_legal_financial, ["id_cliente", "anio_informacion"], how="left"
    )

    df_cc_customer_extra_financing = process_customer_financing_summary_task(
        p_cleaned_cc_customer_extra_financing
    )

    df_customer_features = join_dataframes_task(
        ["id_cliente", "_observ_end_dt"],
        [
            df_base_customer,
            df_income,
            df_ire,
            df_bbk_supplier,
            df_bel_web_login,
            df_bel_app_login,
            df_leads,
            df_cc_customer_extra_financing,
        ],
    )

    df_final_customer = fillna_flags_task(
        df_customer_features,
        [
            "bbk_flag_proveedor",
            "monto_credito_proveedor_flag",
            "antiguedad_cliente_dias",
            "dias_constitucion_empresa",
        ],
    )

    return df_final_customer


@flow(name="customer_flow")
def customer_flow():
    """
    Loads, transforms and saves customer features in the data lake.

    The flow performs the following operations:
    1. Loads raw customer data using the specified date range.
    2. Transforms and processes the customer features data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed
        customer features data.
    """
    raw_data = load_raw_data_flow()

    df_customer_features = transform_customer_task(
        raw_data["cleaned_customer"],
        raw_data["cleaned_employee"],
        raw_data["cleaned_income"],
        raw_data["cleaned_main_address"],
        raw_data["cleaned_ire"],
        raw_data["cleaned_bbk_supplier"],
        raw_data["cleaned_bel_login"],
        raw_data["cleaned_bel_users"],
        raw_data["cleaned_bel_app_master"],
        raw_data["cleaned_leads"],
        raw_data["cleaned_cc_customer_extra_financing"],
        raw_data["cleaned_legal_financial"],
    )

    save_data_flow(df_customer_features)
