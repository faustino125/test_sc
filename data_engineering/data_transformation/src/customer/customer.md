---
description: |
    API documentation for modules: data_engineering.data_transformation.src.customer, data_engineering.data_transformation.src.customer.customer_flow.

lang: en

classoption: oneside
geometry: margin=1in
papersize: a4

linkcolor: blue
links-as-notes: true
...



# Module `data_engineering.data_transformation.src.customer` {#id}





## Sub-modules

* [data_engineering.data_transformation.src.customer.customer_flow](#data_engineering.data_transformation.src.customer.customer_flow)







# Module `data_engineering.data_transformation.src.customer.customer_flow` {#id}







## Functions



### Function `customer_flow` {#id}




>     def customer_flow()


Loads, transforms and saves customer features in the data lake.

The flow performs the following operations:
1. Loads raw customer data using the specified date range.
2. Transforms and processes the customer features data.
3. Saves the processed data to the appropriate environment using the
   specified overwrite strategy.


Returns
-----=
<code>None</code>
:   This flow does not return a value, but saves the processed


customer features data.


### Function `process_bbk_supplier_task` {#id}




>     def process_bbk_supplier_task(
>         p_cleaned_customer: pyspark.sql.dataframe.DataFrame,
>         p_cleaned_bbk_supplier: pyspark.sql.dataframe.DataFrame
>     ) ‑> pyspark.sql.dataframe.DataFrame


Transforms BiBanking supplier data to create a supplier flag exclusively for
legal clients.

This task processes the clean BiBanking supplier and customer data by applying
transformations. It takes in consideration only juridic clients and then
creates a supplier flag only valid to this segment of clients, based on the
<code>descripcion\_operacion</code> field.


Args
-----=
**```p_cleaned_bbk_supplier```** :&ensp;<code>DataFrame</code>
:   Clean BiBanking supplier data.


**```p_cleaned_customer```** :&ensp;<code>DataFrame</code>
:   Cleaned customer data.



Returns
-----=
<code>DataFrame</code>
:   Transformed  bbk customer DataFrame.




### Function `process_bel_app_logins_task` {#id}




>     def process_bel_app_logins_task(
>         p_cleaned_bel_app_login: pyspark.sql.dataframe.DataFrame,
>         p_cleaned_bel_users: pyspark.sql.dataframe.DataFrame
>     ) ‑> pyspark.sql.dataframe.DataFrame


transforms and processes all bel app logins.

This task processes the clean bel app logins data by applying
transformations to it. It standardizes column names, creates new
columns based on transformations, and joins them into a unified dataset.


Args
-----=
**```p_cleaned_bel_app_login```** :&ensp;<code>DataFrame</code>
:   Cleaned bel app login data.


**```p_cleaned_bel_users```** :&ensp;<code>DataFrame</code>
:   Cleaned bel users data.



Returns
-----=
<code>DataFrame</code>
:   Transformed customer bel app logins DataFrame.




### Function `process_bel_web_logins_task` {#id}




>     def process_bel_web_logins_task(
>         p_cleaned_bel_login: pyspark.sql.dataframe.DataFrame,
>         p_cleaned_bel_users: pyspark.sql.dataframe.DataFrame
>     ) ‑> pyspark.sql.dataframe.DataFrame


transforms and processes all bel web logins.

This task processes the clean bel web logins data by applying
transformations to it. It standardizes column names, creates new
columns based on transformations, and joins them into a unified dataset.


Args
-----=
**```p_cleaned_bel_login```** :&ensp;<code>DataFrame</code>
:   Cleaned bel web logins data.


**```p_cleaned_bel_users```** :&ensp;<code>DataFrame</code>
:   Cleaned bel users data.



Returns
-----=
<code>DataFrame</code>
:   Transformed customer bel web logins DataFrame.




### Function `process_customer_financing_summary_task` {#id}




>     def process_customer_financing_summary_task(
>         p_cleaned_cc_customer_extra_financing: pyspark.sql.dataframe.DataFrame
>     ) ‑> pyspark.sql.dataframe.DataFrame


Returns financing information at the customer and general classification levels,
regardless og the specific financing type


Args
-----=
**```p_cleaned_cc_customer_financing```** :&ensp;<code>DataFrame</code>
:   Clean customer financing



Returns
-----=
<code>DataFrame</code>
:   Financing information




### Function `process_customer_task` {#id}




>     def process_customer_task(
>         p_cleaned_customer: pyspark.sql.dataframe.DataFrame,
>         p_cleaned_ire_data: pyspark.sql.dataframe.DataFrame,
>         p_cleaned_employee: pyspark.sql.dataframe.DataFrame
>     ) ‑> pyspark.sql.dataframe.DataFrame


Transforms and processes customer data.

This task processes the clean customer, legal client, and employee data by applying
transformations and transforming steps. It standardizes column names, creates new
columns based on transformations, and joins them into a unified dataset.


Args
-----=
**```p_cleaned_customer```** :&ensp;<code>DataFrame</code>
:   Cleaned customer data.


**```p_cleaned_ire_data```** :&ensp;<code>DataFrame</code>
:   Cleaned legal client data.


**```p_cleaned_employee```** :&ensp;<code>DataFrame</code>
:   Cleaned employee data.



Returns
-----=
<code>DataFrame</code>
:   Transformed customer DataFrame.




### Function `process_income_task` {#id}




>     def process_income_task(
>         p_cleaned_income: pyspark.sql.dataframe.DataFrame
>     ) ‑> pyspark.sql.dataframe.DataFrame


transforms and processes customer data.

This task processes the clean customer income data by applying a sum aggregation
so it can return the sum of the average income in a single column grouped by
customer.


Args
-----=
**```p_cleaned_income```** :&ensp;<code>DataFrame</code>
:   Cleaned customer income data.



Returns
-----=
<code>DataFrame</code>
:   Transformed customer income DataFrame.




### Function `process_ire_task` {#id}




>     def process_ire_task(
>         p_cleaned_ire: pyspark.sql.dataframe.DataFrame
>     ) ‑> pyspark.sql.dataframe.DataFrame


transforms and processes the start of relationship form (IRE) data.

This task processes the clean customer, and IRE data by applying
transformations and transforming steps. It standardizes column names, creates new
columns based on transformations, and joins them into a unified dataset.


Args
-----=
**```p_cleaned_ire```** :&ensp;<code>DataFrame</code>
:   Cleaned ire form data.



Returns
-----=
<code>DataFrame</code>
:   Transformed customer ire DataFrame.




### Function `process_leads_task` {#id}




>     def process_leads_task(
>         p_cleaned_leads: pyspark.sql.dataframe.DataFrame
>     ) ‑> pyspark.sql.dataframe.DataFrame


Transforms leads data to create lead flags exclusively for
legal clients.

This task processes the clean leads and customer data by applying
transformations.


Args
-----=
**```p_cleaned_leads```** :&ensp;<code>DataFrame</code>
:   Clean leads data.



Returns
-----=
<code>DataFrame</code>
:   Transformed leads DataFrame.




### Function `process_legal_financial_task` {#id}




>     def process_legal_financial_task(
>         p_cleaned_legal_financial: pyspark.sql.dataframe.DataFrame
>     ) ‑> pyspark.sql.dataframe.DataFrame


Transforms legal financial data to create legal financial features.

This task processes the clean legal financial data by applying
transformations.


Args
-----=
**```p_cleaned_legal_financial```** :&ensp;<code>DataFrame</code>
:   Clean legal financial data.



Returns
-----=
<code>DataFrame</code>
:   Transformed legal financial DataFrame.




### Function `process_main_address_task` {#id}




>     def process_main_address_task(
>         p_cleaned_main_address: pyspark.sql.dataframe.DataFrame
>     ) ‑> pyspark.sql.dataframe.DataFrame


Transforms and processes customer main address data.

This task processes the clean customer, and main address data by applying
transformations and transforming steps. It standardizes column names, creates new
columns based on transformations, and joins them into a unified dataset.


Args
-----=
**```p_cleaned_main_address```** :&ensp;<code>DataFrame</code>
:   Cleaned customer main address data.



Returns
-----=
<code>DataFrame</code>
:   Transformed customer with main address DataFrame.





-----
Generated by *pdoc* 0.11.1 (<https://pdoc3.github.io>).
