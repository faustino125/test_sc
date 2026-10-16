---
description: |
    API documentation for modules: data_engineering.raw_data.src.missing_dates, data_engineering.raw_data.src.missing_dates.missing_raw_data_flow.

lang: en

classoption: oneside
geometry: margin=1in
papersize: a4

linkcolor: blue
links-as-notes: true
...



# Module `data_engineering.raw_data.src.missing_dates` {#id}





## Sub-modules

* [data_engineering.raw_data.src.missing_dates.missing_raw_data_flow](#data_engineering.raw_data.src.missing_dates.missing_raw_data_flow)







# Module `data_engineering.raw_data.src.missing_dates.missing_raw_data_flow` {#id}

Raw Data Validation.





## Functions



### Function `create_output_df_task` {#id}




>     def create_output_df_task(
>         p_base_df: pyspark.sql.dataframe.DataFrame,
>         p_missing_data: pyspark.sql.dataframe.DataFrame
>     ) ‑> str


Create output dfs.


Args
-----=
**```p_base_path```**
:   Base path inside container


**```p_tableDomain```**
:   general path inside the container



Returns
-----=
<code>str</code>
:   Data lake path.




### Function `create_path_task` {#id}




>     def create_path_task(
>         p_base_path: str,
>         p_tableDomain: str,
>         p_fileName: str,
>         p_date: str
>     ) ‑> str


Create path for Azure Data Lake.


Args
-----=
**```p_base_path```**
:   Base path inside container


**```p_tableDomain```**
:   general path inside the container


**```p_fileName```**
:   vista


**```p_date```**
:   Date string in yyyy/mm format



Returns
-----=
<code>str</code>
:   Data lake path.




### Function `filter_output_df_task` {#id}




>     def filter_output_df_task(
>         p_output_df: pyspark.sql.dataframe.DataFrame,
>         p_missing_in_dwh: pyspark.sql.dataframe.DataFrame
>     ) ‑> pyspark.sql.dataframe.DataFrame


Filter output DataFrame


Args
-----=
**```p_output_df```** :&ensp;<code>df</code>
:   DataFrame with all raw data missing dates


**```p_output_df```** :&ensp;<code>df</code>
:   DataFrame with confirmed missing dates in DWH



Returns
-----=
<code>DataFrame</code>
:   Dataframe with dates to reingest




### Function `get_date_range_task` {#id}




>     def get_date_range_task()


Retrieve all the days from a given date range.


Returns
-----=
<code>DataFrame</code>
:   DataFrame date sequence.




### Function `get_missing_data_task` {#id}




>     def get_missing_data_task(
>         p_raw_data_tables: pyspark.sql.dataframe.DataFrame,
>         p_raw_records_dwh_summary: pyspark.sql.dataframe.DataFrame
>     ) ‑> pyspark.sql.dataframe.DataFrame


Raw data validation.

This task looks in the azure datalake all the missing dates for
all data sources extracted from on-premises DB's.


Args
-----=
**```p_raw_data_tables```** :&ensp;<code>df</code>
:   Raw data sources available in the Azure DL Storage.


**```p_raw_records_dwh_summary```** :&ensp;<code>df</code>
:   Row count for data sources from


on premises DB's.

Returns
-----=
<code>Tuple</code>
:   Processed DataFrames with all the missing dates in each data source.




### Function `load_dwh_missing_data_catalog_task` {#id}




>     def load_dwh_missing_data_catalog_task(
>         p_base_path: str,
>         p_env: str,
>         p_catalog: str,
>         p_df_missing_dates_dwh: pyspark.sql.dataframe.DataFrame
>     ) ‑> pyspark.sql.dataframe.DataFrame


Collect DataFrame to dump as a single json file.


Args
-----=
**```p_base_path```** :&ensp;<code>str</code>
:   Base path to storage account


**```p_env```** :&ensp;<code>str</code>
:   Execution environment


**```p_catalog```** :&ensp;<code>str</code>
:   Catalog in Databricks


**```p_df_missing_dates_dwh```** :&ensp;<code>df</code>
:   DataFrame with all raw data missing in DWH



Returns
-----=
<code>DataFrame</code>
:   Dataframe with all confirmed missing data from DWH




### Function `missing_raw_data_flow` {#id}




>     def missing_raw_data_flow()


Loads, processes, and saves missing raw data information in the datalake.

The flow performs the following operations:
1. Loads raw data sources.
2. Gets the tables that are missing information for a given date.
3. Saves the processed data to the appropriate environment using the specified
overwrite strategy.


Returns
-----=
<code>None</code>
:   This flow does not return a value, but it saves the processed income data.




### Function `read_data_task` {#id}




>     def read_data_task(
>         p_dl_path,
>         p_dateColumn,
>         p_tableName,
>         p_data_type,
>         p_start_dt,
>         p_end_dt
>     )


Read data from a given Data lake path.


Args
-----=
**```p_dl_path```**
:   Data lake path.


**```p_dateColumn```**
:   Date column for raw data


**```p_tableName```**
:   String with historical table Name


**```p_data_type```**
:   String with data type


**```p_start_dt```**
:   Start date for analysis


**```p_end_dt```**
:   End date for analysis



Returns
-----=
<code>DataFrame</code>
:   Dates that have data for the given path.




### Function `standardize_df_task` {#id}




>     def standardize_df_task(
>         p_missing_dates: pyspark.sql.dataframe.DataFrame
>     ) ‑> pyspark.sql.dataframe.DataFrame


Collect DataFrame to dump as a single json file.


Args
-----=
**```p_missing_dates```** :&ensp;<code>df</code>
:   DataFrame with all raw data missing dates



Returns
-----=
<code>DataFrame</code>
:   Dataframe with list of records on single row





-----
Generated by *pdoc* 0.11.1 (<https://pdoc3.github.io>).
