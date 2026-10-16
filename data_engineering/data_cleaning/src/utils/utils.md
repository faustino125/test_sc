---
description: |
    API documentation for modules: data_engineering.data_cleaning.src.utils, data_engineering.data_cleaning.src.utils.external_bureau_utils, data_engineering.data_cleaning.src.utils.utils_data_clean.

lang: en

classoption: oneside
geometry: margin=1in
papersize: a4

linkcolor: blue
links-as-notes: true
...



# Module `data_engineering.data_cleaning.src.utils` {#id}





## Sub-modules

* [data_engineering.data_cleaning.src.utils.external_bureau_utils](#data_engineering.data_cleaning.src.utils.external_bureau_utils)
* [data_engineering.data_cleaning.src.utils.utils_data_clean](#data_engineering.data_cleaning.src.utils.utils_data_clean)







# Module `data_engineering.data_cleaning.src.utils.external_bureau_utils` {#id}







## Functions



### Function `active_type_aggrupation_task` {#id}




>     def active_type_aggrupation_task(
>         p_active_type: pyspark.sql.column.Column
>     ) ‑> pyspark.sql.column.Column


Builts, based on the active type descriptions, a classification that will be
used to build aggrupations later on.

Args
-----=
**```p_active_type```** :&ensp;<code>Column</code>
:   Active type descriptions.



Returns
-----=
<code>Column</code>
:   Active type aggrupation.




### Function `clean_risk_category_task` {#id}




>     def clean_risk_category_task(
>         p_risk_category: pyspark.sql.column.Column
>     ) ‑> pyspark.sql.column.Column


Cleans risk category field.


Args
-----=
**```p_risk_category```** :&ensp;<code>Column</code>
:   Risk category id.



Returns
-----=
<code>Column</code>
:   Standardized risk category id.




### Function `guarantee_type_aggrupations_task` {#id}




>     def guarantee_type_aggrupations_task(
>         p_guarantee_type: pyspark.sql.column.Column
>     ) ‑> pyspark.sql.column.Column


Builts, based on the guarantee type descriptions, a classification
that will be used to build aggrupations later on.


Args
-----=
**```p_guarantee_type```** :&ensp;<code>Column</code>
:   Guarantee type descriptions.



Returns
-----=
<code>Column</code>
:   Guarantee type aggrupations.







# Module `data_engineering.data_cleaning.src.utils.utils_data_clean` {#id}







## Functions



### Function `active_legal_client_flag_task` {#id}




>     def active_legal_client_flag_task(
>         p_std_situation_desc: str,
>         p_total_balance: float
>     ) ‑> pyspark.sql.column.Column


Check some features to validate if a legal client
can be considered as active client


Args
-----=
**```p_std_situation_desc```** :&ensp;<code>DataFrame</code>
:   The account's sitation description


**```p_total_balance```** :&ensp;<code>DataFrame</code>
:   The account's total balance



Returns
-----=
<code>Column</code>
:   Returns 0 value if a legal client is not and active cliente,


otherwise, returns 1


### Function `add_zeros_to_column` {#id}




>     def add_zeros_to_column(
>         p_col_add_z: pyspark.sql.column.Column,
>         p_max_lengt: int
>     ) ‑> pyspark.sql.column.Column


Function to add leading zeros to a Column


Args
-----=
**```p_col_add_z```** :&ensp;<code>Column</code>
:   Column to add leading Zeros


**```p_max_lengt```** :&ensp;<code>int</code>
:   Then Lenght about field.



Returns
-----=
<code>Column</code>
:   Return leading zeros to a Column.




### Function `bad_situation_product_flag_task` {#id}




>     def bad_situation_product_flag_task(
>         p_account_situation: pyspark.sql.column.Column
>     ) ‑> pyspark.sql.column.Column


Check if a product has a bad situation, based on predefined rules.


Args
-----=
**```p_account_situation```** :&ensp;<code>str</code>
:   The account situation of the customer.



Returns
-----=
<code>Column</code>
:   A column with 1 if the product is flagged as bad product,


otherwise 0.


### Function `clean_currency_task` {#id}




>     def clean_currency_task(
>         currency_col: pyspark.sql.column.Column
>     ) ‑> pyspark.sql.column.Column


Standardize transfer currency.


Args
-----=
**```currency_col```** :&ensp;<code>Column</code>
:   Currency description value.



Returns
-----=
<code>Column</code>
:   Standardized column, values could be either gtq, usd or desconocida.




### Function `clean_description_column_task` {#id}




>     def clean_description_column_task(
>         p_col: pyspark.sql.column.Column
>     ) ‑> pyspark.sql.column.Column


Cleans a description column, by applying regular expressions to:
    remove leading and trailing spaces, deleting all characters
    that are not Unicode letters, digits, or spaces, and collapses
    single and multiple internal spaces into a single underscore.

Args
-----=
**```p_col```** :&ensp;<code>Column</code>
:   Column with raw description



Returns
-----=
<code>Column</code>
:   Return processed and clean description.




### Function `clean_flag_task` {#id}




>     def clean_flag_task(
>         p_raw_data: pyspark.sql.dataframe.DataFrame,
>         p_field_alias: str
>     ) ‑> pyspark.sql.dataframe.DataFrame


Clean field values and format it as a flag, using 1 or 0


Args
-----=
**```p_raw_data```** :&ensp;<code>DataFrame</code>
:   The column to be formatted


**```p_field_alias```** :&ensp;<code>str</code>
:   The name of the column in the output DataFrame



Returns
-----=
<code>Column</code>
:   The converted column with flag format.




### Function `convert_to_hex_task` {#id}




>     def convert_to_hex_task(
>         p_column_to_convert: pyspark.sql.column.Column,
>         p_col_alias: str
>     ) ‑> pyspark.sql.column.Column


Converts a column value to hexadecimal format prefixed with '0x'.

This task takes a column from a DataFrame and converts its values to a hexadecimal
string, prefixing it with '0x'. This solves the problem of sensible client IDs that
had already been converted into hexadecimals but cannot be used for
comparisons in joins or filter operations because of conversions performed during
ingestion.


Args
-----=
**```p_column_to_convert```** :&ensp;<code>Column</code>
:   The column whose values are to be converted.


p_col_alias (Column alias identifier):
    - cliente ->  id_cliente
    - cuenta -> cuenta_corporativa
    - Anything else will be considered as the desired alias for the col.

Returns
-----=
<code>Column</code>
:   The converted column with hexadecimal representation.




### Function `days_between_dates_task` {#id}




>     def days_between_dates_task(
>         p_start_date: pyspark.sql.column.Column,
>         p_end_date: pyspark.sql.column.Column
>     ) ‑> pyspark.sql.column.Column


Calculates the integer number of days between two dates.

This task computes the number of days between two dates.
If the result of the months_between calculation is null it returns 0.
Otherwise, it returns the difference in days, of the input dates.


Args
-----=
**```p_start_date```** :&ensp;<code>Column</code>
:   The date from which the calculation starts.


**```p_end_date```** :&ensp;<code>Column</code>
:   The date from which the age is calculated.



Returns
-----=
<code>int</code>
:   The number of full years between the two dates.




### Function `get_cols_task` {#id}




>     def get_cols_task(
>         p_col: pyspark.sql.column.Column,
>         p_var,
>         p_sentence,
>         p_alias,
>         p_type
>     ) ‑> pyspark.sql.column.Column


Function that gets column by when.


Args
-----=
**```col```** :&ensp;<code>Column</code>
:   specific column.


**```var```** :&ensp;<code>\_type\_</code>
:   variable in condition.


**```sentence```** :&ensp;<code>\_type\_</code>
:   sentence of condition.



Returns
-----=
<code>Column</code>
:   result of when.




### Function `get_months_between_task` {#id}




>     def get_months_between_task(
>         p_reference_dt: str,
>         p_second_dt: str,
>         p_field_alias: str
>     )


Calculate the difference in months between two given dates:
(p_reference_dt - p_second_dt).


Args
-----=
**```p_reference_dt```** :&ensp;<code>date</code>
:   reference date.


**```p_second_dt```** :&ensp;<code>date</code>
:   second date.


**```p_field_alias```** :&ensp;<code>str</code>
:   Field alias.



Returns
-----=
<code>Column</code>
:   difference in months casted as int.




### Function `merge_dif_schema_task` {#id}




>     def merge_dif_schema_task(
>         p_df_list: list
>     ) ‑> pyspark.sql.dataframe.DataFrame


Performs a union of DataFrames with different schema.


Args
-----=
**```p_df_list```** :&ensp;<code>list</code>
:   The list of DataFrames to be unified.



Returns
-----=
<code>DataFrame</code>
:   The unified DataFrames based on p_df_list.




### Function `replace_null_or_empty_values` {#id}




>     def replace_null_or_empty_values()


Replaces null or empty values inside DataFrame with a <code>desconocido</code>
value. It will not affect numerical or date type values.


Returns
-----=
<code>DataFrame</code>
:   DataFrame with empty and null values replaced with <code>desconocido</code>.




### Function `std_situation_account_task` {#id}




>     def std_situation_account_task(
>         p_orig_situation_desc: pyspark.sql.column.Column
>     ) ‑> pyspark.sql.column.Column


Using the original situation account data,
return a standarized situation account description


Args
-----=
**```p_orig_situation_desc```** :&ensp;<code>DataFrame</code>
:   The column to be formated



Returns
-----=
<code>Column</code>
:   The converted column with standarized situation description.




### Function `sum_fields_task` {#id}




>     def sum_fields_task(
>         p_field_list: list
>     )


Function to sum multiple fields.

Args:
p_field_list (int list): Values that we want to sum

Returns:
sum: Sum of the elements inside the p_field_list


### Function `years_between_dates_task` {#id}




>     def years_between_dates_task(
>         p_start_date: str,
>         p_end_date: str
>     ) ‑> int


Calculates the integer number of years between two dates.

This task computes the number of years between two dates.
If the result of the months_between calculation is null it returns 0.
Otherwise, it returns the floor division of the months by 12 to get the number of
full years.


Args
-----=
**```p_start_date```** :&ensp;<code>str</code>
:   The date from which the calculation starts.


**```p_end_date```** :&ensp;<code>str</code>
:   The date from which the age is calculated.



Returns
-----=
<code>int</code>
:   The number of full years between the two dates.





-----
Generated by *pdoc* 0.11.1 (<https://pdoc3.github.io>).
