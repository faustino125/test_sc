from prefect import flow, task
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as f

from data_engineering.core.load_data_flow import load_raw_data_flow
from data_engineering.core.save_data_flow import save_data_flow
from data_engineering.core.utils import arrange_columns
from data_engineering.data_cleaning.src.utils.utils_data_clean import (
    convert_to_hex_task,
    replace_null_or_empty_values,
)


@task(name="get_cc_category_task", tags=["column", "get_cc_category"])
def cc_category_task(p_cc_cat: Column, p_dict, p_cc_brand=None) -> Column:
    """Get the category of a credit card.

    Args:
        p_cc_cat: column with raw cc category
        p_dict: dictionary to compare
        p_cc_brand: brand column

    Returns:
        Column: column with id_category, category or next_category
    """
    when_expr = f.when(f.lit(False), None)
    for key, value in p_dict.items():
        if p_cc_brand:
            when_expr = (
                when_expr.when(p_cc_cat.isin(value), key)
                .when(
                    (p_cc_cat == "platinum") & (f.col(p_cc_brand) == "visa"),
                    "signature",
                )
                .when(
                    (p_cc_cat == "platinum") & (f.col(p_cc_brand) == "mc"),
                    "black",
                )
            )
        else:
            when_expr = when_expr.when(p_cc_cat.isin(value), key)

    when_expr = when_expr.otherwise("desconocido")

    return when_expr


@replace_null_or_empty_values()
@arrange_columns(p_start_cols=["fecha_catalogo", "id_categoria_tc"])
@task(name="clean_cc_categories_task", tags=["data cleaning", "preprocessing"])
def clean_cc_categories_task(p_raw_data_categories: DataFrame) -> DataFrame:
    """
    Cleans and processes credit card categories data.

    This task processes the raw credit card categories data by
    applying transformations and cleaning steps.
    It standardizes certain fields, creates new calculated columns,
    and fills empty values with
    default values.

    Args:
        p_raw_data_categories (DataFrame): Raw credit card categories data.

    Returns:
        DataFrame: Processed credit card categories DataFrame.
    """

    dict_cc_cat = {
        "classic": "CLASICA",
        "gold": ["ORO", "GOLD"],
        "platinum": "PLATINUM",
        "standard": "STANDARD",
        "premier": "PREMIER",
        "infinite": "INFINITE",
        "signature": "SIGNATURE",
        "black": "BLACK",
    }

    dict_id_cc_cat = {
        1: ["classic", "standard"],
        2: ["premier", "gold"],
        3: "platinum",
        4: ["signature", "black"],
        5: "infinite",
        0: "desconocido",
    }

    dict_cc_next_category = {
        "premier": "classic",
        "gold": "standard",
        "platinum": ["premier", "gold"],
    }

    ls_business_cc = [
        "TARJETA DE CREDITO EMPRESARIAL CLASICA BC",
        "TARJETA DE CREDITO EMPRESARIAL PLATINUM BC",
        "TARJETA DE CREDITO PURCHASING",
        "TARJETA DE CREDITO MC EMPRESARIAL",
        "TARJETA DE CREDITO COMBUSTIBLE BC",
        "TARJETA DE CREDITO ABASTO",
        "TARJETA DE CREDITO INSTITUCIONAL",
        "TARJETA DE CREDITO CLASICA 7",
        "TARJETA DE CREDITO PLATINUM EMPRESARIAL",
        "TARJETA DE CREDITO COMPRAS BC",
        "TARJETA DE CREDITO EXENTA IVA INSTITUCIONAL",
        "TARJETA DE CREDITO MC EXECUTIVE BUSINESS INTERNACIONAL",
        "TARJETA DE CREDITO EMPRESARIAL",
        "TARJETA DE CREDITO CLASICA 8",
        "TARJETA DE CREDITO CTA BC",
        "TARJETA DE CREDITO EMPRESARIAL PLATINUM PLUS",
        "TARJETA DE CREDITO CORPORATE BC",
    ]

    df_cc_categories = p_raw_data_categories.select(
        convert_to_hex_task(f.col("categoria"), "id_categoria_tc"),
        f.col("fecha_catalogo"),
        f.lower(f.col("segmento")).alias("segmento"),
        f.when(f.col("marca").contains("MASTERCARD"), "mc")
        .otherwise(f.lower(f.col("marca")))
        .alias("emisor"),
        f.when(f.col("grupo") == "CLASICA", "classic")
        .when(
            ((f.col("grupo") == "ORO") | (f.col("grupo") == "GOLD"))
            & (f.col("marca").contains("VISA")),
            "premier",
        )
        .when(f.col("grupo") == "ORO", "gold")
        .otherwise(f.coalesce(f.lower(f.col("grupo")), f.lit("desconocido")))
        .alias("descripcion_categoria"),
        cc_category_task(
            f.when(
                ((f.col("grupo") == "ORO") | (f.col("grupo") == "GOLD"))
                & (f.col("marca").contains("VISA")),
                "PREMIER",
            )
            .when(f.col("grupo").contains(" SV"), "SIGNATURE")
            .otherwise(f.trim(f.regexp_replace(f.col("grupo"), " UEFA", ""))),
            p_dict=dict_cc_cat,
        ).alias("categoria_estandard"),
        f.col("aplica_upgrade"),
        f.lower(f.col("dw_producto_codigo_descripcion")).alias("descripcion_producto"),
        f.when(f.col("dw_producto_codigo_descripcion").contains("INSTITUCIONAL"), 1)
        .otherwise(0)
        .alias("institucional_flag"),
        f.when(f.col("dw_producto_codigo_descripcion").isin(ls_business_cc), 1)
        .otherwise(0)
        .alias("empresarial_flag"),
        f.col("fecha_catalogo"),
    )

    df_cc_categories = df_cc_categories.select(
        f.col("*"),
        cc_category_task(p_cc_cat=f.col("categoria_estandard"), p_dict=dict_id_cc_cat)
        .cast("int")
        .alias("id_categoria"),
        cc_category_task(
            p_cc_cat=f.col("categoria_estandard"),
            p_dict=dict_cc_next_category,
            p_cc_brand="emisor",
        ).alias("tc_siguiente_categoria"),
        f.when(f.col("aplica_upgrade").isNotNull(), f.col("aplica_upgrade"))
        .otherwise("N")
        .alias("flag_cambio_categoria"),
        f.when(f.col("descripcion_categoria") != "bi online", 0)
        .otherwise(1)
        .alias("bi_online_flag"),
    )

    return df_cc_categories


@flow(name="cc_categories_flow")
def cc_categories_flow():
    """
    Load, process, and save credit card categories data in the data lake.

    This flow performs the following operations:
    1. Loads raw credit card categories data using the specified date range.
    2. Cleans and processes the credit card categories data.
    3. Saves the processed data to the appropriate environment using the
       specified overwrite strategy.

    Returns:
        None: This flow does not return a value, but saves the processed credit
        card categories data.
    """
    raw_data = load_raw_data_flow()

    df_cc_categories = raw_data["raw_cc_categories"]

    df_cc_categories = clean_cc_categories_task(df_cc_categories)

    save_data_flow(df_cc_categories)
