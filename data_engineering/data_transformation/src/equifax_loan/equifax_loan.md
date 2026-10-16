---
description: |
    API documentation for modules: data_engineering.data_transformation.src.equifax_loan, data_engineering.data_transformation.src.equifax_loan.eql_income_flow, data_engineering.data_transformation.src.equifax_loan.eql_person_flow, data_engineering.data_transformation.src.equifax_loan.eql_request_flow, data_engineering.data_transformation.src.equifax_loan.eql_sib_debt_detail_flow, data_engineering.data_transformation.src.equifax_loan.eql_sib_debt_history_flow, data_engineering.data_transformation.src.equifax_loan.eql_tu_debt_detail_flow.

lang: en

classoption: oneside
geometry: margin=1in
papersize: a4

linkcolor: blue
links-as-notes: true
...



# Module `data_engineering.data_transformation.src.equifax_loan` {#id}





## Sub-modules

* [data_engineering.data_transformation.src.equifax_loan.eql_income_flow](#data_engineering.data_transformation.src.equifax_loan.eql_income_flow)
* [data_engineering.data_transformation.src.equifax_loan.eql_person_flow](#data_engineering.data_transformation.src.equifax_loan.eql_person_flow)
* [data_engineering.data_transformation.src.equifax_loan.eql_request_flow](#data_engineering.data_transformation.src.equifax_loan.eql_request_flow)
* [data_engineering.data_transformation.src.equifax_loan.eql_sib_debt_detail_flow](#data_engineering.data_transformation.src.equifax_loan.eql_sib_debt_detail_flow)
* [data_engineering.data_transformation.src.equifax_loan.eql_sib_debt_history_flow](#data_engineering.data_transformation.src.equifax_loan.eql_sib_debt_history_flow)
* [data_engineering.data_transformation.src.equifax_loan.eql_tu_debt_detail_flow](#data_engineering.data_transformation.src.equifax_loan.eql_tu_debt_detail_flow)







# Module `data_engineering.data_transformation.src.equifax_loan.eql_income_flow` {#id}







## Functions



### Function `customer_income_task` {#id}




>     def customer_income_task(
>         p_df_person_request: pyspark.sql.dataframe.DataFrame
>     ) ‑> pyspark.sql.dataframe.DataFrame


Returns customer's income


Args
-----=
**```p_df_person_request```**
:   person request



Returns
-----=
<code>DataFrame</code>
:   customer's income




### Function `eql_income_flow` {#id}




>     def eql_income_flow()


Loads, transforms and saves debt detail features in the data lake.

The flow performs the following operations:
1. Loads cleaned data using the specified date range.
2. Transforms and processes the data.
3. Saves the processed data to the appropriate environment using the
   specified overwrite strategy.


Returns
-----=
<code>None</code>
:   This flow does not return a value, but saves the processed


debt detail features data.





# Module `data_engineering.data_transformation.src.equifax_loan.eql_person_flow` {#id}







## Functions



### Function `eql_person_flow` {#id}




>     def eql_person_flow()


Loads, transforms and saves debt detail features in the data lake.

The flow performs the following operations:
1. Loads cleaned data using the specified date range.
2. Transforms and processes the data.
3. Saves the processed data to the appropriate environment using the
   specified overwrite strategy.


Returns
-----=
<code>None</code>
:   This flow does not return a value, but saves the processed


debt detail features data.





# Module `data_engineering.data_transformation.src.equifax_loan.eql_request_flow` {#id}







## Functions



### Function `eql_request_flow` {#id}




>     def eql_request_flow()


Loads, transforms and saves request's features in the data lake.

The flow performs the following operations:
1. Loads cleaned data using the specified date range.
2. Transforms and processes the data.
3. Saves the processed data to the appropriate environment using the
   specified overwrite strategy.


Returns
-----=
<code>None</code>
:   This flow does not return a value, but saves the processed


request's features data.


### Function `request_task` {#id}




>     def request_task(
>         p_person_request: pyspark.sql.dataframe.DataFrame,
>         p_person: pyspark.sql.dataframe.DataFrame,
>         p_customer: pyspark.sql.dataframe.DataFrame
>     ) ‑> pyspark.sql.dataframe.DataFrame


Returns person request


Args
-----=
**```p_person_request```** :&ensp;<code>DataFrame</code>
:   Person request data


**```p_person```** :&ensp;<code>DataFrame</code>
:   Person data


**```p_customer```** :&ensp;<code>DataFrame</code>
:   Customer data



Returns
-----=
<code>DataFrame</code>
:   person request's DataFrame.




### Function `sib_loan_account_task` {#id}




>     def sib_loan_account_task(
>         p_loan_account: pyspark.sql.dataframe.DataFrame,
>         p_loan_case: pyspark.sql.dataframe.DataFrame
>     ) ‑> pyspark.sql.dataframe.DataFrame


Returns loan account number


Args
-----=
**```p_loan_account```** :&ensp;<code>DataFrame</code>
:   Loan account data


**```p_loan_case```** :&ensp;<code>DataFrame</code>
:   Loan Case data



Returns
-----=
<code>DataFrame</code>
:   Sib loan account DataFrame.







# Module `data_engineering.data_transformation.src.equifax_loan.eql_sib_debt_detail_flow` {#id}







## Functions



### Function `eql_sib_debt_detail_flow` {#id}




>     def eql_sib_debt_detail_flow()


Loads, transforms and saves debt detail features in the data lake.

The flow performs the following operations:
1. Loads cleaned data using the specified date range.
2. Transforms and processes the data.
3. Saves the processed data to the appropriate environment using the
   specified overwrite strategy.


Returns
-----=
<code>None</code>
:   This flow does not return a value, but saves the processed


debt detail features data.


### Function `liabilities_task` {#id}




>     def liabilities_task(
>         p_df_sib_debt: pyspark.sql.dataframe.DataFrame,
>         p_type_debt: str,
>         p_pivot_col: str
>     ) ‑> pyspark.sql.dataframe.DataFrame


Transforms and processes direct liabilities by asset type.


Args
-----=
**```p_df_sib_debt```**
:   sib debt


**```p_type_debt```**
:   Type debt


**```p_pivot_col```**
:   Pivot column



Returns
-----=
<code>DataFrame</code>
:   principal and secondary applicant




### Function `sib_debt_history_task` {#id}




>     def sib_debt_history_task(
>         p_df_sib_debt_his: pyspark.sql.dataframe.DataFrame,
>         p_df_liabilities: pyspark.sql.dataframe.DataFrame,
>         p_type_debt: str
>     ) ‑> pyspark.sql.dataframe.DataFrame


Returns the worst debt category


Args
-----=
**```p_df_sib_debt_hib```**
:   sib debt history


**```p_type_debt```**
:   type debt



Returns
-----=
<code>DataFrame</code>
:   date of the debt




### Function `type_debt_task` {#id}




>     def type_debt_task(
>         p_type_debt: str
>     ) ‑> pyspark.sql.dataframe.DataFrame


Classify the type of debt


Args
-----=
**```p_type_debt```**
:   Type debt



Returns
-----=
<code>DataFrame</code>
:   Type of debt




### Function `worst_debt_category_task` {#id}




>     def worst_debt_category_task(
>         p_df_sib_debt: pyspark.sql.dataframe.DataFrame,
>         p_type_debt: str
>     ) ‑> pyspark.sql.dataframe.DataFrame


Returns the worst debt category


Args
-----=
**```p_df_sib_debt```**
:   sib debt


**```p_type_debt```**
:   Type debt



Returns
-----=
<code>DataFrame</code>
:   worst debt category







# Module `data_engineering.data_transformation.src.equifax_loan.eql_sib_debt_history_flow` {#id}







## Functions



### Function `customer_income_task` {#id}




>     def customer_income_task(
>         p_df_person_request: pyspark.sql.dataframe.DataFrame
>     ) ‑> pyspark.sql.dataframe.DataFrame


Returns customer's income


Args
-----=
**```p_df_person_request```**
:   person request



Returns
-----=
<code>DataFrame</code>
:   customer's income




### Function `eql_sib_debt_history_flow` {#id}




>     def eql_sib_debt_history_flow()


Loads, transforms and saves debt history features in the data lake.

The flow performs the following operations:
1. Loads cleaned data using the specified date range.
2. Transforms and processes the data.
3. Saves the processed data to the appropriate environment using the
   specified overwrite strategy.


Returns
-----=
<code>None</code>
:   This flow does not return a value, but saves the processed


debt history features data.


### Function `liabilities_task` {#id}




>     def liabilities_task(
>         p_df_sib_debt: pyspark.sql.dataframe.DataFrame,
>         p_type_debt: str,
>         p_pivot_col: str
>     ) ‑> pyspark.sql.dataframe.DataFrame


Transforms and processes direct liabilities by asset type.


Args
-----=
**```p_df_sib_debt```**
:   sib debt


**```p_type_debt```**
:   Type debt


**```p_pivot_col```**
:   Pivot column



Returns
-----=
<code>DataFrame</code>
:   principal and secondary applicant




### Function `type_debt_task` {#id}




>     def type_debt_task(
>         p_type_debt: str
>     ) ‑> pyspark.sql.dataframe.DataFrame


Classify the type of debt


Args
-----=
**```p_type_debt```**
:   Type debt



Returns
-----=
<code>DataFrame</code>
:   Type of debt




### Function `worst_debt_category_task` {#id}




>     def worst_debt_category_task(
>         p_df_sib_debt: pyspark.sql.dataframe.DataFrame,
>         p_type_debt: str
>     ) ‑> pyspark.sql.dataframe.DataFrame


Returns the worst debt category


Args
-----=
**```p_df_sib_debt```**
:   sib debt


**```p_type_debt```**
:   Type debt



Returns
-----=
<code>DataFrame</code>
:   worst debt category







# Module `data_engineering.data_transformation.src.equifax_loan.eql_tu_debt_detail_flow` {#id}







## Functions



### Function `eql_tu_debt_detail_flow` {#id}




>     def eql_tu_debt_detail_flow()


Loads, transforms and saves debt detail features in the data lake.

The flow performs the following operations:
1. Loads cleaned data using the specified date range.
2. Transforms and processes the data.
3. Saves the processed data to the appropriate environment using the
   specified overwrite strategy.


Returns
-----=
<code>None</code>
:   This flow does not return a value, but saves the processed


debt detail features data.


### Function `unify_currency_task` {#id}




>     def unify_currency_task(
>         p_df_tu_debt: pyspark.sql.dataframe.DataFrame,
>         p_cleaned_exchange_rate: pyspark.sql.dataframe.DataFrame
>     ) ‑> pyspark.sql.dataframe.DataFrame


Unifies various currencies in a field


Args
-----=
**```p_df_sib_debt```**
:   sib debt


**```p_cleaned_exchange_rate```**
:   Exchange rate



Returns
-----=
<code>DataFrame</code>
:   Unified currency





-----
Generated by *pdoc* 0.11.5 (<https://pdoc3.github.io>).
