---
description: |
    API documentation for modules: data_engineering.data_transformation.src.sib, data_engineering.data_transformation.src.sib.sib_debt_detail_flow, data_engineering.data_transformation.src.sib.sib_debt_history_flow.

lang: en

classoption: oneside
geometry: margin=1in
papersize: a4

linkcolor: blue
links-as-notes: true
...



# Namespace `data_engineering.data_transformation.src.sib` {#id}





## Sub-modules

* [data_engineering.data_transformation.src.sib.sib_debt_detail_flow](#data_engineering.data_transformation.src.sib.sib_debt_detail_flow)
* [data_engineering.data_transformation.src.sib.sib_debt_history_flow](#data_engineering.data_transformation.src.sib.sib_debt_history_flow)







# Module `data_engineering.data_transformation.src.sib.sib_debt_detail_flow` {#id}







## Functions



### Function `debt_type_task` {#id}




>     def debt_type_task(
>         p_debt_type: str,
>         p_df_worst_risk: pyspark.sql.dataframe.DataFrame,
>         p_risk_categories: pyspark.sql.dataframe.DataFrame
>     ) ‑> pyspark.sql.dataframe.DataFrame


Classify the debt type


Args
-----=
**```p_debt_type```** :&ensp;<code>str</code>
:   Debt type.


**```p_df_worst_risk```** :&ensp;<code>DataFrame</code>
:   Worst risk category for each debt type.


**```p_risk_categories```** :&ensp;<code>DataFrame</code>
:   Risk categories.



Returns
-----=
<code>DataFrame</code>
:   Classified worst debt for each debt type.




### Function `pre_process_debt_datail_task` {#id}




>     def pre_process_debt_datail_task(
>         p_cleaned_sib_debt_detail: pyspark.sql.dataframe.DataFrame,
>         p_cleaned_sib_general_data: pyspark.sql.dataframe.DataFrame
>     ) ‑> pyspark.sql.dataframe.DataFrame


Get latest debts data.


Args
-----=
**```p_cleaned_sib_debt_detail```** :&ensp;<code>DataFrame</code>
:   Cleaned SIB


debt detail data.
**```p_cleaned_sib_general_data```** :&ensp;<code>DataFrame</code>
:   Cleaned debt general data.



Returns
-----=
<code>DataFrame</code>
:   Debt detail unique debts DataFrame.




### Function `process_debt_conditions_task` {#id}




>     def process_debt_conditions_task(
>         p_cleaned_sib_debt_detail: pyspark.sql.dataframe.DataFrame,
>         p_credit_card_products: List
>     ) ‑> pyspark.sql.dataframe.DataFrame


Transforms and processes debt conditions data.

This task processes the clean SIB debt detail data by applying
transformations and transforming steps. It standardizes column names, creates new
columns based on transformations and aggregations.


Args
-----=
**```p_cleaned_sib_debt_detail```** :&ensp;<code>DataFrame</code>
:   Cleaned SIB


debt detail data.
**```p_credit_card_products```** :&ensp;<code>List</code>
:   Credit card products names.



Returns
-----=
<code>DataFrame</code>
:   Transformed debt conditions DataFrame.




### Function `process_debt_status_task` {#id}




>     def process_debt_status_task(
>         p_cleaned_sib_debt_detail: pyspark.sql.dataframe.DataFrame
>     ) ‑> pyspark.sql.dataframe.DataFrame


Transforms and processes debt status data.

This task processes the clean SIB debt detail data by applying
transformations and transforming steps. It standardizes column names, creates new
columns based on transformations and aggregations.


Args
-----=
**```p_cleaned_sib_debt_detail```** :&ensp;<code>DataFrame</code>
:   Cleaned SIB


debt detail data.

Returns
-----=
<code>DataFrame</code>
:   Transformed debt status features DataFrame.




### Function `process_worst_debt_category_task` {#id}




>     def process_worst_debt_category_task(
>         p_cleaned_sib_debt_detail: pyspark.sql.dataframe.DataFrame,
>         p_cleaned_sib_risk_categories: pyspark.sql.dataframe.DataFrame
>     ) ‑> pyspark.sql.dataframe.DataFrame


Transforms and processes worst debt category data.

This task processes the clean SIB debt detail data by applying
transformations and transforming steps. It standardizes column names, creates new
columns based on transformations and aggregations.


Args
-----=
**```p_cleaned_sib_debt_detail```** :&ensp;<code>DataFrame</code>
:   Cleaned SIB


debt detail data.
**```p_cleaned_sib_risk_categories```** :&ensp;<code>DataFrame</code>
:   SIB risk categories.



Returns
-----=
<code>DataFrame</code>
:   Transformed worst debt category DataFrame.




### Function `sib_debt_detail_flow` {#id}




>     def sib_debt_detail_flow()


Loads, transforms and saves SIB debt detail data features
in the data lake.

The flow performs the following operations:
1. Loads SIB debt detail data using the specified date range.
2. Transforms and processes the SIB debt detail features data.
3. Saves the processed data to the appropriate environment using the
   specified overwrite strategy.


Returns
-----=
<code>None</code>
:   This flow does not return a value, but saves the processed


SIB debt detail features data.





# Module `data_engineering.data_transformation.src.sib.sib_debt_history_flow` {#id}







## Functions



### Function `process_debt_history_task` {#id}




>     def process_debt_history_task(
>         p_cleaned_sib_debt_history: pyspark.sql.dataframe.DataFrame,
>         p_cleaned_sib_debt_detail: pyspark.sql.dataframe.DataFrame,
>         p_cleaned_sib_risk_category: pyspark.sql.dataframe.DataFrame
>     ) ‑> pyspark.sql.dataframe.DataFrame


Transforms and processes SIB historical data.

This task processes the clean SIB debt detail and debt history data
by applying transformations and transforming steps. It standardizes column names,
creates new columns based on transformations and aggregations.


Args
-----=
**```p_cleaned_sib_debt_history```** :&ensp;<code>DataFrame</code>
:   Cleaned SIB


debt history data.
**```p_cleaned_sib_debt_detail```** :&ensp;<code>DataFrame</code>
:   Cleaned SIB


debt detail data.
**```p_cleaned_sib_risk_category```** :&ensp;<code>DataFrame</code>
:   Cleaned SIB


risk categories.

Returns
-----=
<code>DataFrame</code>
:   Transformed SIB history DataFrame.




### Function `sib_debt_history_flow` {#id}




>     def sib_debt_history_flow()


Loads, transforms and saves SIB debt historical data features in the data lake.

The flow performs the following operations:
1. Loads SIB debt data data using the specified date range.
2. Transforms and processes the SIB debt data features data.
3. Saves the processed data to the appropriate environment using the
   specified overwrite strategy.


Returns
-----=
<code>None</code>
:   This flow does not return a value, but saves the processed


SIB debt historical features data.



-----
Generated by *pdoc* 0.11.1 (<https://pdoc3.github.io>).
