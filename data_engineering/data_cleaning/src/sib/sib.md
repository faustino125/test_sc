---
description: |
    API documentation for modules: data_engineering.data_cleaning.src.sib, data_engineering.data_cleaning.src.sib.sib_debt_detail_flow, data_engineering.data_cleaning.src.sib.sib_debt_history_flow, data_engineering.data_cleaning.src.sib.sib_entry_point_flow, data_engineering.data_cleaning.src.sib.sib_general_data_flow, data_engineering.data_cleaning.src.sib.sib_risk_categories_flow.

lang: en

classoption: oneside
geometry: margin=1in
papersize: a4

linkcolor: blue
links-as-notes: true
...



# Module `data_engineering.data_cleaning.src.sib` {#id}





## Sub-modules

* [data_engineering.data_cleaning.src.sib.sib_debt_detail_flow](#data_engineering.data_cleaning.src.sib.sib_debt_detail_flow)
* [data_engineering.data_cleaning.src.sib.sib_debt_history_flow](#data_engineering.data_cleaning.src.sib.sib_debt_history_flow)
* [data_engineering.data_cleaning.src.sib.sib_entry_point_flow](#data_engineering.data_cleaning.src.sib.sib_entry_point_flow)
* [data_engineering.data_cleaning.src.sib.sib_general_data_flow](#data_engineering.data_cleaning.src.sib.sib_general_data_flow)
* [data_engineering.data_cleaning.src.sib.sib_risk_categories_flow](#data_engineering.data_cleaning.src.sib.sib_risk_categories_flow)







# Module `data_engineering.data_cleaning.src.sib.sib_debt_detail_flow` {#id}







## Functions



### Function `payment_frequency_task` {#id}




>     def payment_frequency_task(
>         col: pyspark.sql.column.Column
>     ) ‑> pyspark.sql.column.Column


Crates description for payment type.


Args
-----=
**```col```** :&ensp;<code>Column</code>
:   Payment id.



Returns
-----=
<code>Column</code>
:   Payment type description




### Function `sib_debt_detail_flow` {#id}




>     def sib_debt_detail_flow()


Load, process, and save SIB debt detail
in the data lake.

The flow performs the following operations:
1. Loads SIB debt detail raw data.
2. Cleans and processes the SIB debt detail.
3. Saves the processed data to the appropriate environment using the
   specified overwrite strategy.


Returns
-----=
<code>None</code>
:   This flow does not return a value, but saves the processed


SIB debt detail.





# Module `data_engineering.data_cleaning.src.sib.sib_debt_history_flow` {#id}







## Functions



### Function `sib_debt_history_flow` {#id}




>     def sib_debt_history_flow()


Load, process, and save SIB debt history data
in the data lake.

The flow performs the following operations:
1. Loads SIB debt history raw data.
2. Cleans and processes the SIB debt history data.
3. Saves the processed data to the appropriate environment using the
   specified overwrite strategy.


Returns
-----=
<code>None</code>
:   This flow does not return a value, but saves the processed


SIB debt history data.





# Module `data_engineering.data_cleaning.src.sib.sib_entry_point_flow` {#id}







## Functions



### Function `clean_sib_entry_point_task` {#id}




>     def clean_sib_entry_point_task(
>         p_sib_entry_point: pyspark.sql.dataframe.DataFrame
>     ) ‑> pyspark.sql.dataframe.DataFrame


Cleans and processes SIB customer id to BI customer id.

This task processes the SIB customer id to BI customer id raw
data, by applying transformations and cleaning steps.
It select the necessary fields.


Args
-----=
**```p_sib_entry_point```** :&ensp;<code>DataFrame</code>
:   SIB customer id to BI customer id


raw data.

Returns
-----=
<code>DataFrame</code>
:   Processed SIB customer id to BI customer id DataFrame.




### Function `sib_entry_point_flow` {#id}




>     def sib_entry_point_flow()


Load, process, and save SIB customer id to BI customer id data
in the data lake.

The flow performs the following operations:
1. Loads SIB customer id to BI customer id raw data.
2. Cleans and processes the SIB SIB customer id to BI customer id data.
3. Saves the processed data to the appropriate environment using the
   specified overwrite strategy.


Returns
-----=
<code>None</code>
:   This flow does not return a value, but saves the processed


SIB customer id to BI customer id data.





# Module `data_engineering.data_cleaning.src.sib.sib_general_data_flow` {#id}







## Functions



### Function `sib_general_data_flow` {#id}




>     def sib_general_data_flow()


Load, process, and save SIB summary data
in the data lake.

The flow performs the following operations:
1. Loads SIB summary raw data.
2. Cleans and processes the SIB summary data.
3. Saves the processed data to the appropriate environment using the
   specified overwrite strategy.


Returns
-----=
<code>None</code>
:   This flow does not return a value, but saves the processed


SIB summary data.





# Module `data_engineering.data_cleaning.src.sib.sib_risk_categories_flow` {#id}







## Functions



### Function `clean_sib_risk_categories_task` {#id}




>     def clean_sib_risk_categories_task(
>         p_risk_categories: pyspark.sql.dataframe.DataFrame
>     ) ‑> pyspark.sql.dataframe.DataFrame


Cleans and processes SIB risk categories data.

This task processes the SIB risk categories raw data,
by applying transformations and cleaning steps.
It select the necessary fields.


Args
-----=
**```p_risk_categories```** :&ensp;<code>DataFrame</code>
:   SIB risk categories raw data.



Returns
-----=
<code>DataFrame</code>
:   Processed SIB risk categories products DataFrame.




### Function `sib_risk_categories_flow` {#id}




>     def sib_risk_categories_flow()


Load, process, and save SIB risk categories data
in the data lake.

The flow performs the following operations:
1. Loads SIB risk categories raw data.
2. Cleans and processes the SIB risk categories data.
3. Saves the processed data to the appropriate environment using the
   specified overwrite strategy.


Returns
-----=
<code>None</code>
:   This flow does not return a value, but saves the processed


SIB risk categories data.



-----
Generated by *pdoc* 0.11.1 (<https://pdoc3.github.io>).
