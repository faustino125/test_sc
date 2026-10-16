---
description: |
    API documentation for modules: data_engineering.data_cleaning.src.customer, data_engineering.data_cleaning.src.customer.customer_flow, data_engineering.data_cleaning.src.customer.income_flow, data_engineering.data_cleaning.src.customer.leads_iniciativa_flow, data_engineering.data_cleaning.src.customer.legal_financial_information_flow, data_engineering.data_cleaning.src.customer.main_address_flow, data_engineering.data_cleaning.src.customer.products_flow.

lang: en

classoption: oneside
geometry: margin=1in
papersize: a4

linkcolor: blue
links-as-notes: true
...



# Module `data_engineering.data_cleaning.src.customer` {#data_engineering.data_cleaning.src.customer}





## Sub-modules

* [data_engineering.data_cleaning.src.customer.customer_flow](#data_engineering.data_cleaning.src.customer.customer_flow)
* [data_engineering.data_cleaning.src.customer.income_flow](#data_engineering.data_cleaning.src.customer.income_flow)
* [data_engineering.data_cleaning.src.customer.leads_iniciativa_flow](#data_engineering.data_cleaning.src.customer.leads_iniciativa_flow)
* [data_engineering.data_cleaning.src.customer.legal_financial_information_flow](#data_engineering.data_cleaning.src.customer.legal_financial_information_flow)
* [data_engineering.data_cleaning.src.customer.main_address_flow](#data_engineering.data_cleaning.src.customer.main_address_flow)
* [data_engineering.data_cleaning.src.customer.products_flow](#data_engineering.data_cleaning.src.customer.products_flow)







# Module `data_engineering.data_cleaning.src.customer.customer_flow` {#data_engineering.data_cleaning.src.customer.customer_flow}







## Functions



### Function `blacklisted_professions_flag_task` {#data_engineering.data_cleaning.src.customer.customer_flow.blacklisted_professions_flag_task}




>     def blacklisted_professions_flag_task(
>         p_economic_activity: str,
>         p_profession: str,
>         p_work: str
>     ) ‑> pyspark.sql.column.Column


Flags if a p_ is blacklisted based on rules defined by BI Credit.

This task checks if the provided profession, economic activity, or work field
contains any blacklisted professions that are restricted. The blacklisted
professions are predefined in the task logic.


Args
-----=
**```p_economic_activity```** :&ensp;<code>str</code>
:   The economic activity of the customer.


**```p_profession```** :&ensp;<code>str</code>
:   The profession of the customer.


**```p_work```** :&ensp;<code>str</code>
:   The work description of the customer.



Returns
-----=
<code>Column</code>
:   A column with 1 if the profession is blacklisted, otherwise 0.




### Function `customer_flow` {#data_engineering.data_cleaning.src.customer.customer_flow.customer_flow}




>     def customer_flow()


Load, process, and save customer data in the data lake.

The flow performs the following operations:
1. Loads raw customer data using the specified date range.
2. Cleans and processes the customer data.
3. Saves the processed data to the appropriate environment using the
   specified overwrite strategy.


Returns
-----=
<code>None</code>
:   This flow does not return a value, but saves the processed


customer data.





# Module `data_engineering.data_cleaning.src.customer.income_flow` {#data_engineering.data_cleaning.src.customer.income_flow}







## Functions



### Function `income_flow` {#data_engineering.data_cleaning.src.customer.income_flow.income_flow}




>     def income_flow()


Load, process, and save income data in the data lake.

The flow performs the following operations:
1. Loads raw income data using the specified date range.
2. Cleans and processes the income data.
3. Saves the processed data to the appropriate environment using the
   specified overwrite strategy.


Returns
-----=
<code>None</code>
:   This flow does not return a value, but saves the processed


income data.





# Module `data_engineering.data_cleaning.src.customer.leads_iniciativa_flow` {#data_engineering.data_cleaning.src.customer.leads_iniciativa_flow}







## Functions



### Function `exclusion_campanias` {#data_engineering.data_cleaning.src.customer.leads_iniciativa_flow.exclusion_campanias}




>     def exclusion_campanias(
>         p_raw_exclusion_campanias: pyspark.sql.dataframe.DataFrame
>     ) ‑> pyspark.sql.dataframe.DataFrame


Transforms and selects relevant.


Parameters
-----=
p_raw_exclusion_campanias (DataFrame): The input DataFrame
containing exclusion campaign data.


Returns
-----=
<code>DataFrame</code>
:   A DataFrame with selected and transformed columns.




### Function `leads_iniciativa_flow` {#data_engineering.data_cleaning.src.customer.leads_iniciativa_flow.leads_iniciativa_flow}




>     def leads_iniciativa_flow()


Load, process, and save customer leads iniciativa data in the data lake.

The flow performs the following operations:
1. Loads customers investment account raw data using the specified date range.
2. Cleans and processes the customers investment account data.
3. Saves the processed data to the appropriate environment using the
   specified overwrite strategy.


Returns
-----=
<code>None</code>
:   This flow does not return a value, but saves the processed


customers investment account data.





# Module `data_engineering.data_cleaning.src.customer.legal_financial_information_flow` {#data_engineering.data_cleaning.src.customer.legal_financial_information_flow}







## Functions



### Function `legal_financial_information_flow` {#data_engineering.data_cleaning.src.customer.legal_financial_information_flow.legal_financial_information_flow}




>     def legal_financial_information_flow()


Loads, processes, and saves legal client's financial information data in datalake.

The flow performs the following operations:
1. Loads raw financial information data.
2. Cleans and processes financial information data.
3. Saves the processed data to the appropriate environment
   using the specified overwrite strategy.


Returns
-----=
<code>None</code>
:   This flow does not return a value,


but it saves the processed financial information data.





# Module `data_engineering.data_cleaning.src.customer.main_address_flow` {#data_engineering.data_cleaning.src.customer.main_address_flow}







## Functions



### Function `main_address_flow` {#data_engineering.data_cleaning.src.customer.main_address_flow.main_address_flow}




>     def main_address_flow()


Loads, processes, and saves addresses data in datalake.

The flow performs the following operations:
1. Loads raw addresses data using the specified date range.
2. Cleans and processes the addresses data.
3. Saves the processed data to the appropriate environment
   using the specified overwrite strategy.


Returns
-----=
<code>None</code>
:   This flow does not return a value,


but it saves the processed addresses data.





# Module `data_engineering.data_cleaning.src.customer.products_flow` {#data_engineering.data_cleaning.src.customer.products_flow}







## Functions



### Function `products_flow` {#data_engineering.data_cleaning.src.customer.products_flow.products_flow}




>     def products_flow()


Load, process, and save customer data in the data lake.

The flow performs the following operations:
1. Loads raw customer data using the specified date range.
2. Cleans and processes the customer products data.
3. Saves the processed data to the appropriate environment using the
   specified overwrite strategy.


Returns
-----=
<code>None</code>
:   This flow does not return a value, but saves the processed


customer data.



-----
Generated by *pdoc* 0.11.6 (<https://pdoc3.github.io>).
