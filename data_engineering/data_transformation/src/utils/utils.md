---
description: |
    API documentation for modules: data_engineering.data_transformation.src.utils, data_engineering.data_transformation.src.utils.eqf_utils, data_engineering.data_transformation.src.utils.external_bureau_utils, data_engineering.data_transformation.src.utils.utils.

lang: en

classoption: oneside
geometry: margin=1in
papersize: a4

linkcolor: blue
links-as-notes: true
...



# Module `data_engineering.data_transformation.src.utils` {#id}





## Sub-modules

* [data_engineering.data_transformation.src.utils.eqf_utils](#data_engineering.data_transformation.src.utils.eqf_utils)
* [data_engineering.data_transformation.src.utils.external_bureau_utils](#data_engineering.data_transformation.src.utils.external_bureau_utils)
* [data_engineering.data_transformation.src.utils.utils](#data_engineering.data_transformation.src.utils.utils)







# Module `data_engineering.data_transformation.src.utils.eqf_utils` {#id}







## Functions



### Function `applicant_task` {#id}




>     def applicant_task(
>         p_df_person_request: pyspark.sql.dataframe.DataFrame,
>         p_df_person: pyspark.sql.dataframe.DataFrame,
>         p_cleaned_customer: pyspark.sql.dataframe.DataFrame
>     ) ‑> pyspark.sql.dataframe.DataFrame


Transforms and processes information over principal and secondary applicant.


Args
-----=
**```p_df_person_request```**
:   person request data


**```p_df_person```**
:   person data


**```p_cleaned_customer```**
:   customer data



Returns
-----=
<code>DataFrame</code>
:   principal and secondary applicant




### Function `lookup_external_bureau_task` {#id}




>     def lookup_external_bureau_task(
>         p_external_bureau: pyspark.sql.dataframe.DataFrame,
>         p_reference_bureau: str
>     ) ‑> pyspark.sql.dataframe.DataFrame


Process to group data by four grouping fields and selects the row
with the latest date and id


Args
-----=
**```p_external_bureau```** :&ensp;<code>DataFrame</code>
:   External bureau data


**```p_reference_bureau```** :&ensp;<code>str</code>
:   type of bureau



Returns
-----=
<code>DataFrame</code>
:   Dataframe  with a search by request and bureau type







# Module `data_engineering.data_transformation.src.utils.external_bureau_utils` {#id}







## Functions



### Function `sib_entry_point_mapping` {#id}




>     def sib_entry_point_mapping(
>         p_customer: pyspark.sql.dataframe.DataFrame,
>         p_sib_entry_point: pyspark.sql.dataframe.DataFrame,
>         p_sib_data: pyspark.sql.dataframe.DataFrame
>     ) ‑> pyspark.sql.dataframe.DataFrame


Maps external bureau id to BI customer id.


Args
-----=
**```p_customer```**
:   BI Customers.


**```p_sib_entry_point```**
:   Map between SIB persona id and BI customer id


**```p_sib_data```**
:   SIB data



Returns
-----=
<code>DataFrame</code>
:   SIB data with BI customer id.







# Module `data_engineering.data_transformation.src.utils.utils` {#id}







## Functions



### Function `add_exchange_rate_task` {#id}




>     def add_exchange_rate_task(
>         p_base_data: pyspark.sql.dataframe.DataFrame,
>         p_exchange_rate: pyspark.sql.dataframe.DataFrame,
>         p_snapshot=False
>     ) ‑> pyspark.sql.dataframe.DataFrame


Adds the exchange rate information to a dataframe based on the <code>p\_snapshot</code>
to detect whether the join clause needs to be done based on <code>\_observ\_end\_dt</code>
or <code>fecha\_transaccion</code>.

Parameters:
p_base_data (DataFrame): Column name that represents the currency ID.
p_exchange_rate (DataFrame): Dataframe containing the daily exchange rate.
p_snapshot (bool): Flag set by default to <code>False</code> that decides the join strategy.

Returns:
DataFrame: <code>p\_base\_data</code> Dataframe with the daily exchange rate aadditioned inside
the <code>tasa\_cambio</code> column.


### Function `convert_currency_to_gtq_task` {#id}




>     def convert_currency_to_gtq_task(
>         col_name: str,
>         exchange_col_name: str,
>         col_cast: str = 'float'
>     ) ‑> pyspark.sql.dataframe.DataFrame


Convert a column in USD to GTQ.


Parameters
-----=
col_name (str): Other currency column name.
exchange_col_name (str): Column with exchange rate
col_cast  (str): Type to cast the column.


Returns
-----=
<code>DataFrame</code>
:   dataframe with new flag column.




### Function `create_currency_col_task` {#id}




>     def create_currency_col_task(
>         p_currency_col: str,
>         p_col_name: str,
>         p_exchange_rate: str,
>         p_using_description: bool = False
>     ) ‑> list


Creates a list of columns by splitting the currency column.


Args
-----=
**```p_currency_col```** :&ensp;<code>str</code>
:   The name of the column containing currency information.


**```p_col_name```** :&ensp;<code>str</code>
:   The name of the column containing amounts.


**```p_exchange_rate```** :&ensp;<code>str</code>
:   The name of the column containing exchange rates.


**```p_using_description```** :&ensp;<code>bool</code>, optional
:   Whether to use the description.



Returns
-----=
<code>list</code>
:   A list of columns resulting from the task.




### Function `get_min_max_date_task` {#id}




>     def get_min_max_date_task(
>         p_date_data: pyspark.sql.dataframe.DataFrame,
>         p_date_field: str,
>         p_date_alias: str,
>         p_operation='min',
>         p_group_columns=['id_cliente', '_observ_end_dt']
>     ) ‑> pyspark.sql.dataframe.DataFrame


Function that gets the first or last date of a groupby dataframe.


Args
-----=
**```p_date_data```** :&ensp;<code>DataFrame</code>
:   DataFrame containing the data to proccess..


**```p_date_field```** :&ensp;<code>str</code>
:   Name of field date in dataframe.


**```p_date_alias```** :&ensp;<code>str</code>
:   New field alias for the calculated column.


**```p_operation```** :&ensp;<code>str</code>
:   Optional parameter for getting min or max date, it is set to


<code>min</code>.
by default but it can be changed to <code>max</code>.
**```p_group_columns```** :&ensp;<code>list</code>
:   Optional parameter for the column names that will be


used as base for the aggregation, if not changed, it will do the group by
clause based on <code>id\_cliente</code> and <code>\_observ\_end\_dt</code>.

Returns
-----=
<code>DataFrame</code>
:   Field with first or last date of a dataframe.




### Function `join_dataframes_task` {#id}




>     def join_dataframes_task(
>         p_base_columns: list,
>         p_dataframes: list,
>         p_how='left'
>     ) ‑> pyspark.sql.dataframe.DataFrame


Recursively joins dataframes while respecting the order of p_dataframes

This task joins each dataframe in order from the position 0 of
<code>p\_dataframes</code> to N, using as join reference <code>p\_base\_columns</code>, which need to
already exist inside each dataframe of <code>p\_dataframes</code> in order to work as
expected.


Args
-----=
**```p_base_columns```** :&ensp;<code>list</code>
:   The columns in which the join will be based.


**```p_dataframes```** :&ensp;<code>list</code>
:   The list of dataframes that will be joined.


**```p_how```** :&ensp;<code>str</code>
:   The joi strategy that will be used, set by default to


<code>left</code>.

Returns
-----=
<code>DataFrame</code>
:   The final joined dataframe in order from position 0


of p_dataframes to position N.


### Function `split_currency_task` {#id}




>     def split_currency_task(
>         p_value_col: str,
>         p_currency_col: str,
>         p_using_description: str = False
>     ) ‑> list


Generates columns for GTQ and USD currencies in a given DataFrame.

This task process the value of <code>p\_value\_col</code> to split it into two
columns with suffix _usd or _gtq based on the currency id given by
<code>p\_currency\_col</code>.


Args
-----=
**```p_value_col```** :&ensp;<code>DataFrame</code>
:   Name of the column we want to split.


**```p_currency_col```** :&ensp;<code>DataFrame</code>
:   Name of the column that has the currency


id in it

Returns
-----=
<code>list</code>
:   List of the splitted columns with its corresponding suffixes.




### Function `suffix_columns_task` {#id}




>     def suffix_columns_task(
>         p_data: pyspark.sql.dataframe.DataFrame,
>         p_suffix: str
>     )


Function that gets all the columns ending with an specified
<code>p\_suffix</code> substring.


Args
-----=
**```p_data```** :&ensp;<code>DataFrame</code>
:   DataFrame containing the data to proccess.


**```p_suffix```** :&ensp;<code>str</code>
:   Suffix to be searched in the <code>p\_data</code> columns.



Returns
-----=
<code>list</code>
:   List containing dataframe columns that match the specified suffix.




### Function `sum_cond_task` {#id}




>     def sum_cond_task(
>         p_cond: pyspark.sql.column.Column,
>         p_col: Any
>     ) ‑> pyspark.sql.column.Column


Generalization of an abstract condition where a <code>sum</code> aggregation
when a <code>when</code> clause is needed.

Parameters:
p_cond (Column): Conditional clause that will delimit the <code>sum</code> clause
p_col (Any): Column value that will be added to the result if the conditional
clause is True.

Returns:
Column: A new column with the computed conditional sum aggregation.


### Function `unified_currency_col_task` {#id}




>     def unified_currency_col_task(
>         p_currency_id: str,
>         p_transaction_amount: str,
>         p_exchange_rate: str
>     ) ‑> pyspark.sql.column.Column


Computes a unified amount in GTQ based on the USD currency exchange rate.

Parameters:
p_currency_id (str): Column name that represents the currency ID.
p_transaction_amount (str): Column name that represents the transaction amount.
p_exchange_rate (str): Column name that represents the exchange rate.

Returns:
Column: A new column with the computed unified amount.



-----
Generated by *pdoc* 0.11.1 (<https://pdoc3.github.io>).
