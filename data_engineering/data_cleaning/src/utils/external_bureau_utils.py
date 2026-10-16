from prefect import task
from pyspark.sql import Column
from pyspark.sql import functions as f


@task(
    name="active_type_aggrupation_task",
    tags=["data cleaning", "preprocessing", "equifax bulky"],
)
def active_type_aggrupation_task(p_active_type: Column) -> Column:
    """
    Builts, based on the active type descriptions, a classification that will be
    used to build aggrupations later on.
    Args:
        p_active_type (Column): Active type descriptions.
    Returns:
        Column: Active type aggrupation.
    """
    return (
        f.when(p_active_type == "tarjeta_de_credito", "tc")
        .when(p_active_type == "tarjeta_de_credito_factorada", "tc_factorada")
        .when(
            p_active_type
            == "activo_crediticio_vinculado_a_tarjeta_de_credito_convenios_de_pago",
            "tc_convenios_de_pago",
        )
        .when(
            p_active_type.contains("activo_crediticio_vinculado_a_tarjeta_de_credito"),
            "tc_extrafinanciamientos",
        )
        .when(
            p_active_type == "credito_en_cuenta_de_depositos_monetarios",
            "creditos_cta_dep_monetarios",
        )
        .when(p_active_type == "prestamos", p_active_type)
        .otherwise("otras_deudas")
    )


@task(
    name="guarantee_type_aggrupations_task",
    tags=["data cleaning", "preprocessing", "equifax bulky"],
)
def guarantee_type_aggrupations_task(p_guarantee_type: Column) -> Column:
    """
    Builts, based on the guarantee type descriptions, a classification
    that will be used to build aggrupations later on.

    Args:
        p_guarantee_type (Column): Guarantee type descriptions.

    Returns:
        Column: Guarantee type aggrupations.
    """
    return (
        f.when(p_guarantee_type.like("%fideicomiso%"), "fideicomisos")
        .when(
            p_guarantee_type.like("%fiduciari%")
            & p_guarantee_type.like("%bienes inmuebles%"),
            "bienes_inmuebles_fiduciarios",
        )
        .when(
            p_guarantee_type.like("%fiduciari%"),
            "fiduciarios",
        )
        .when(
            p_guarantee_type.like("%bienes inmuebles%"),
            "bienes_inmuebles",
        )
        .when(
            p_guarantee_type.like("%otras garantias%"),
            "otras_garantias",
        )
        .when(
            p_guarantee_type.like("%prendas%"),
            "prendas",
        )
        .when(p_guarantee_type.like("%fondo de garantia%"), "fondo_de_garantia")
        .when(
            p_guarantee_type.like("%operaciones autoliquidables%"),
            "operaciones_autoliquidables",
        )
    )


@task(
    name="clean_risk_category_task",
    tags=["data cleaning", "preprocessing", "external bureau"],
)
def clean_risk_category_task(p_risk_category: Column) -> Column:
    """
    Cleans risk category field.

    Args:
        p_risk_category (Column): Risk category id.

    Returns:
        Column: Standardized risk category id.
    """
    return (
        f.when(f.trim(p_risk_category).isin(["S/C", "-", ""]), "S")
        .otherwise(f.trim(p_risk_category))
        .alias("id_categoria_riesgo")
    )
