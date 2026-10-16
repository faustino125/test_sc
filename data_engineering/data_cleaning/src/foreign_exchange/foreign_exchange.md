---
description: |
    API documentation for modules: data_engineering.data_cleaning.src.foreign_exchange, data_engineering.data_cleaning.src.foreign_exchange.daily_exchange_flow, data_engineering.data_cleaning.src.foreign_exchange.forex_transaction_flow.

lang: en

classoption: oneside
geometry: margin=1in
papersize: a4

linkcolor: blue
links-as-notes: true
...



# Module `data_engineering.data_cleaning.src.foreign_exchange` {#id}





## Sub-modules

* [data_engineering.data_cleaning.src.foreign_exchange.daily_exchange_flow](#data_engineering.data_cleaning.src.foreign_exchange.daily_exchange_flow)
* [data_engineering.data_cleaning.src.foreign_exchange.forex_transaction_flow](#data_engineering.data_cleaning.src.foreign_exchange.forex_transaction_flow)







# Module `data_engineering.data_cleaning.src.foreign_exchange.daily_exchange_flow` {#id}







## Functions



### Function `clean_daily_exchange_task` {#id}




>     def clean_daily_exchange_task(
>         p_raw_exchange_rate: pyspark.sql.dataframe.DataFrame
>     ) ‑> pyspark.sql.dataframe.DataFrame


Ingest daily exchange rate data into foreign exchange data cleaning layer.


Args
-----=
**```p_raw_exchange_rate```** :&ensp;<code>DataFrame</code>
:   Daily exchange rate raw data.



Returns
-----=
<code>DataFrame</code>
:   Processed daily exchange rate DataFrame.




### Function `daily_exchange_flow` {#id}




>     def daily_exchange_flow()


Loads, processes, and saves daily exchange data in datalake.

The flow performs the following operations:
1. Loads raw daily exchange data using the specified date range.
2. Cleans and processes the daily exchange data.
3. Saves the processed data to the appropriate environment
   using the specified overwrite strategy.


Returns
-----=
<code>None</code>
:   This flow does not return a value, but it saves the processed daily


exchange data.





# Module `data_engineering.data_cleaning.src.foreign_exchange.forex_transaction_flow` {#id}







## Functions



### Function `clean_forex_branches_task` {#id}




>     def clean_forex_branches_task(
>         p_raw_forex_branches: pyspark.sql.dataframe.DataFrame
>     ) ‑> pyspark.sql.dataframe.DataFrame


Cleans and processes foreign exchange digital channel data by applying
transformations and cleaningsteps. It standardizes certain fields, creates
new calculated columns, and fills empty values with 0.


Args
-----=
**```p_raw_forex_branches```** :&ensp;<code>DataFrame</code>
:   Raw foreign exchange digital channel data.



Returns
-----=
<code>DataFrame</code>
:   The processed foreign exchange digital channel DataFrame.




### Function `clean_forex_digital_task` {#id}




>     def clean_forex_digital_task(
>         p_raw_forex_digital: pyspark.sql.dataframe.DataFrame
>     ) ‑> pyspark.sql.dataframe.DataFrame


Cleans and processes foreign exchange digital channel data by applying
transformations and cleaning steps. It standardizes certain fields,
creates new calculated columns, and fills empty values with 0.


Args
-----=
**```p_raw_forex_digital```** :&ensp;<code>DataFrame</code>
:   Raw foreign exchange digital channel data.



Returns
-----=
<code>DataFrame</code>
:   The processed foreign exchange digital channel DataFrame.




### Function `clean_forex_fields_task` {#id}




>     def clean_forex_fields_task(
>         p_conditional_field: str,
>         p_purchases_value: str,
>         p_sales_value: str,
>         p_type_value: str,
>         p_channel: str
>     ) ‑> pyspark.sql.column.Column


Ingest daily exchange rate data into foreign exchange data cleaning layer.


Args
-----=
**```p_conditional_field```** :&ensp;<code>str</code>
:   Column to compare


**```p_purchases_value```** :&ensp;<code>str</code>
:   Purchase value to return


**```p_sales_value```** :&ensp;<code>str</code>
:   Sales value to return


**```alias_field```** :&ensp;<code>str</code>
:   Alias of the resulting column


**```p_type_value```** :&ensp;<code>str</code>
:   Type of the returned column


**```p_channel```** :&ensp;<code>str</code>
:   Foreign exchange channel type



Returns
-----=
<code>DataFrame</code>
:   Dataframe with debit and credit column merged.




### Function `clean_forex_trading_task` {#id}




>     def clean_forex_trading_task(
>         p_raw_forex_trading: pyspark.sql.dataframe.DataFrame
>     ) ‑> pyspark.sql.dataframe.DataFrame


Cleans and processes foreign exchange trading data by applying transformations
and cleaning steps. It standardizes certain fields, creates new calculated
columns, and fills empty values with 0.


Args
-----=
**```p_raw_forex_trading```** :&ensp;<code>DataFrame</code>
:   Raw foreign exchange trading data.



Returns
-----=
<code>DataFrame</code>
:   The processed foreign exchange trading data DataFrame.




### Function `evaluate_condition_task` {#id}




>     def evaluate_condition_task(
>         p_value_condition: str
>     ) ‑> list


Evaluate foreign exchange channel condition so we can standarize the operation
description.


Args
-----=
**```p_value_condition```** :&ensp;<code>str</code>
:   Value to compare.



Returns
-----=
values (list[str]): List with types of operations.


### Function `forex_transaction_flow` {#id}




>     def forex_transaction_flow()


Loads, processes, and saves forex transactions data in datalake.

The flow performs the following operations:
1. Loads raw forex transaction channels data using the specified date range.
2. Cleans and processes the forex transactions data.
3. Saves the processed data to the appropriate environment
   using the specified overwrite strategy.


Returns
-----=
<code>None</code>
:   This flow does not return a value,but it saves


the processed transactions data.


### Function `merge_forex_fields_task` {#id}




>     def merge_forex_fields_task(
>         p_conditional_field: str,
>         p_purchases_field: str,
>         p_sales_field: str,
>         p_channel: str
>     ) ‑> pyspark.sql.column.Column


Merges foreign exchange fields and gives them an alias.


Args
-----=
**```p_conditional_field```** :&ensp;<code>str</code>
:   Column to compare.


**```p_purchases_field```** :&ensp;<code>str</code>
:   Debit column to merge.


**```p_sales_field```** :&ensp;<code>str</code>
:   Credit column to merge.


**```p_channel```** :&ensp;<code>str</code>
:   Forex p_channel type.



Returns
-----=
A dataframe with debit and credit column merged.



-----
Generated by *pdoc* 0.11.5 (<https://pdoc3.github.io>).
