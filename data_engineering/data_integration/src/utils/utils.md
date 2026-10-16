---
description: |
    API documentation for modules: data_engineering.data_integration.src.utils, data_engineering.data_integration.src.utils.customer_utils, data_engineering.data_integration.src.utils.utils.

lang: en

classoption: oneside
geometry: margin=1in
papersize: a4

linkcolor: blue
links-as-notes: true
...



# Module `data_engineering.data_integration.src.utils` {#data_engineering.data_integration.src.utils}





## Sub-modules

* [data_engineering.data_integration.src.utils.customer_utils](#data_engineering.data_integration.src.utils.customer_utils)
* [data_engineering.data_integration.src.utils.utils](#data_engineering.data_integration.src.utils.utils)







# Module `data_engineering.data_integration.src.utils.customer_utils` {#data_engineering.data_integration.src.utils.customer_utils}







## Functions



### Function `legal_segment_mapping_task` {#data_engineering.data_integration.src.utils.customer_utils.legal_segment_mapping_task}




>     def legal_segment_mapping_task(
>         df_legal: pyspark.sql.dataframe.DataFrame
>     ) ‑> pyspark.sql.dataframe.DataFrame


Maps legal segments based on income, catchment, and credit transactions.


Args
-----=
**```df_legal```** :&ensp;<code>DataFrame</code>
:   DataFrame containing legal segment information.



Returns
-----=
<code>DataFrame</code>
:   A DataFrame with legal segment mapping.




### Function `tier_level_expr` {#data_engineering.data_integration.src.utils.customer_utils.tier_level_expr}




>     def tier_level_expr(
>         column_name: str,
>         tiers: tuple
>     )


Build a tier expression for a numeric column using three thresholds.





# Module `data_engineering.data_integration.src.utils.utils` {#data_engineering.data_integration.src.utils.utils}







## Functions



### Function `add_suffix_to_columns_task` {#data_engineering.data_integration.src.utils.utils.add_suffix_to_columns_task}




>     def add_suffix_to_columns_task(
>         p_data: pyspark.sql.dataframe.DataFrame,
>         p_suffix: str,
>         p_exclusions=[]
>     )


Function that gets all the columns ending with an specified
<code>p\_suffix</code> substring.


Args
-----=
**```p_data```** :&ensp;<code>DataFrame</code>
:   DataFrame containing the data to proccess.


**```p_suffix```** :&ensp;<code>str</code>
:   Suffix to be added in the <code>p\_data</code> columns.


**```p_exclusions```** :&ensp;<code>str</code>
:   Columns that are not being renamed.



Returns
-----=
<code>DataFrame</code>
:   <code>p\_data</code> containing renamed columns.




### Function `compute_sum_with_fixed_and_variants` {#data_engineering.data_integration.src.utils.utils.compute_sum_with_fixed_and_variants}




>     def compute_sum_with_fixed_and_variants(
>         p_df: pyspark.sql.dataframe.DataFrame,
>         p_new_col: str,
>         p_fixed_substring: str,
>         p_variable_substrings: List[str]
>     ) ‑> pyspark.sql.dataframe.DataFrame


Adds a new column as the sum of all integer columns whose names contain:
- a fixed substring (e.g., 'prestamos_cnt')
- and at least one variable substring (e.g., ['en_mora', 'juridico']).

Keeps all original columns and appends the new one.
If 'p_new_col' already exists, it is dropped before being recreated.
If no matching columns are found, the new column will contain 0.


Args
-----=
**```p_df```** :&ensp;<code>DataFrame</code>
:   Source DataFrame.


**```p_new_col```** :&ensp;<code>str</code>
:   Name of the new column to create.


**```p_fixed_substring```** :&ensp;<code>str</code>
:   Fixed substring required in column names.


**```p_variable_substrings```** :&ensp;<code>List\[str]</code>
:   Variable substrings to match.



Returns
-----=
<code>DataFrame</code>
:   DataFrame with the new column added.




### Function `fill_na` {#data_engineering.data_integration.src.utils.utils.fill_na}




>     def fill_na(
>         p_df: pyspark.sql.dataframe.DataFrame
>     ) ‑> pyspark.sql.dataframe.DataFrame


Replaces null values in numeric columns with zero.

This task scans the input DataFrame and identifies columns with numeric
data types ('int', 'bigint', 'float', 'double','decimal'). All null values in these
columns are replaced with '0'.


Args
-----=
**```p_df```** :&ensp;<code>DataFrame</code>
:   The input DataFrame containing potential null values.



Returns
-----=
<code>DataFrame</code>
:   The DataFrame with null values in numeric columns replaced


by zero.


### Function `get_top_by_order` {#data_engineering.data_integration.src.utils.utils.get_top_by_order}




>     def get_top_by_order(
>         p_df: pyspark.sql.dataframe.DataFrame,
>         p_partition_cols: List[str],
>         p_order_cols: List[str],
>         p_order_type: str
>     ) ‑> pyspark.sql.dataframe.DataFrame


Returns the top-1 row per partition based on the specified order.


Args
-----=
**```p_df```** :&ensp;<code>DataFrame</code>
:   Input DataFrame.


**```p_partition_cols```** :&ensp;<code>List\[str]</code>
:   Columns to partition by.


**```p_order_cols```** :&ensp;<code>List\[str]</code>
:   Columns to order by.


**```p_order_type```** :&ensp;<code>str</code>
:   "asc" or "desc" to define the ordering.



Returns
-----=
<code>DataFrame</code>
:   DataFrame with only the top-1 row per partition.




### Function `join_dataframes_task` {#data_engineering.data_integration.src.utils.utils.join_dataframes_task}




>     def join_dataframes_task(
>         p_base_columns: list,
>         p_dataframes: list,
>         p_how='left'
>     ) ‑> pyspark.sql.dataframe.DataFrame


Recursively joins dataframes while respecting the order of p_dataframes

This task joins each dataframe in order from the position 0 of
'p_dataframes' to N, using as join reference 'p_base_columns', which need to
already exist inside each dataframe of 'p_dataframes' in order to work as
expected.


Args
-----=
**```p_base_columns```** :&ensp;<code>list</code>
:   The columns in which the join will be based.


**```p_dataframes```** :&ensp;<code>list</code>
:   The list of dataframes that will be joined.


**```p_how```** :&ensp;<code>str</code>
:   The join strategy that will be used, set by default to


'left'.

Returns
-----=
<code>DataFrame</code>
:   The final joined dataframe in order from position 0


of p_dataframes to position N.


### Function `remove_prefix_from_columns_task` {#data_engineering.data_integration.src.utils.utils.remove_prefix_from_columns_task}




>     def remove_prefix_from_columns_task(
>         p_df: pyspark.sql.dataframe.DataFrame,
>         p_prefix: str
>     ) ‑> pyspark.sql.dataframe.DataFrame


Renames all the columns from <code>p\_df</code> and removes the <code>p\_prefix</code> prefix.

Iterates over the columns, slicing its names if the prefix <code>p\_prefix</code>
exist at the start of the string of the column name, if the previous,
condition is not met, the function will leave the column name as it
is.


Args
-----=
**```p_df```** :&ensp;<code>DataFrame</code>
:   The input DataFrame with a prefix to be removed


from certain columns.
**```p_prefix```** :&ensp;<code>DataFrame</code>
:   The prefix to be removed from <code>p\_df</code> column


names.

Returns
-----=
<code>DataFrame</code>
:   The DataFrame with the renamed columns




### Function `remove_suffix_from_columns_task` {#data_engineering.data_integration.src.utils.utils.remove_suffix_from_columns_task}




>     def remove_suffix_from_columns_task(
>         p_df: pyspark.sql.dataframe.DataFrame,
>         p_suffix: str
>     ) ‑> pyspark.sql.dataframe.DataFrame


Renames all the columns from <code>p\_df</code> and removes the <code>p\_suffix</code> suffix.

Iterates over the columns, slicing its names if the suffix <code>p\_suffix</code>
exist at the end of the string of the column name, if the previous,
condition is not met, the function will leave the column name as it
is.


Args
-----=
**```p_df```** :&ensp;<code>DataFrame</code>
:   The input DataFrame with a suffix to be removed


from certain columns.
**```p_suffix```** :&ensp;<code>DataFrame</code>
:   The suffix to be removed from <code>p\_df</code> column


names.

Returns
-----=
<code>DataFrame</code>
:   The DataFrame with the renamed columns




### Function `sum_features_across_dataframes_task` {#data_engineering.data_integration.src.utils.utils.sum_features_across_dataframes_task}




>     def sum_features_across_dataframes_task(
>         p_features_to_sum: List[Tuple[pyspark.sql.dataframe.DataFrame, str]],
>         p_join_keys: List[str],
>         p_result_col: str,
>         p_how: str = 'full',
>         p_replace_negative: bool = False
>     ) ‑> pyspark.sql.dataframe.DataFrame


Joins multiple DataFrames and sums the specified feature columns
into a single new column.

For each (DataFrame, column_name) pair in 'p_features_to_sum', the
function performs a join between all DataFrames using 'p_join_keys',
then sums all the feature columns using coalesce to treat nulls as zero.
If all feature columns are null for a given row, the result will be null.


Args
-----=
**```p_features_to_sum```** :&ensp;<code>List\[Tuple\[DataFrame, str]]</code>
:   List of tuples where
    each tuple contains a DataFrame and the name of the column to sum.


**```p_join_keys```** :&ensp;<code>List\[str]</code>
:   Column names used as join keys
    (e.g. ["id_cliente", "_observ_end_dt"]).


**```p_result_col```** :&ensp;<code>str</code>
:   Name of the resulting column containing the sum.


**```p_how```** :&ensp;<code>str</code>
:   Join strategy. Defaults to "full" to preserve all clients
    across all DataFrames regardless of which sources they appear in.


**```p_replace_negative```** :&ensp;<code>bool</code>
:   If True, negative values in the result
    column are replaced with 0. Defaults to False.



Returns
-----=
<code>DataFrame</code>
:   Joined DataFrame with a new column 'p_result_col'


containing the total sum of all feature columns. If all feature
columns are null, the result will be null.



-----
Generated by *pdoc* 0.11.6 (<https://pdoc3.github.io>).
