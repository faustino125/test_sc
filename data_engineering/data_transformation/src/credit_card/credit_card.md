---
description: |
    API documentation for modules: data_engineering.data_transformation.src.credit_card, data_engineering.data_transformation.src.credit_card.additional_credit_card_flow, data_engineering.data_transformation.src.credit_card.credit_card_flow.

lang: en

classoption: oneside
geometry: margin=1in
papersize: a4

linkcolor: blue
links-as-notes: true
...



# Module `data_engineering.data_transformation.src.credit_card` {#id}





## Sub-modules

* [data_engineering.data_transformation.src.credit_card.additional_credit_card_flow](#data_engineering.data_transformation.src.credit_card.additional_credit_card_flow)
* [data_engineering.data_transformation.src.credit_card.credit_card_flow](#data_engineering.data_transformation.src.credit_card.credit_card_flow)







# Module `data_engineering.data_transformation.src.credit_card.additional_credit_card_flow` {#id}







## Functions



### Function `additional_credit_card_flow` {#id}




>     def additional_credit_card_flow()


Loads, transforms and saves additional credit card features in the data lake.

The flow performs the following operations:
1. Loads cleaned additional credit card data using the specified date range.
2. Transforms and processes the additional credit card features data.
3. Saves the processed data to the appropriate environment using the
   specified overwrite strategy.


Returns
-----=
<code>None</code>
:   This flow does not return a value, but saves the processed


additional credit card features data.





# Module `data_engineering.data_transformation.src.credit_card.credit_card_flow` {#id}







## Functions



### Function `calculate_cc_current_month_financing_amounts_task` {#id}




>     def calculate_cc_current_month_financing_amounts_task(
>         p_cleaned_cc_extra_financing: pyspark.sql.dataframe.DataFrame
>     ) ‑> pyspark.sql.dataframe.DataFrame


Calculates the financing amounts granted during the current month for each
currency, broken down by financing type


Args
-----=
**```p_cleaned_cc_extra_financing```** :&ensp;<code>DataFrame</code>
:   Cleaned cc financing data.



Returns
-----=
<code>DataFrame</code>
:   A Dataframe containing the financing amounts granted during


the current month.


### Function `calculates_active_financing_amount_metrics_task` {#id}




>     def calculates_active_financing_amount_metrics_task(
>         p_cleaned_cc_extra_financing: pyspark.sql.dataframe.DataFrame
>     ) ‑> pyspark.sql.dataframe.DataFrame


Calculates active financing metrics by financing type and currency.


Args
-----=
**```p_cleaned_cc_extra_financing```** :&ensp;<code>DataFrame</code>
:   Cleaned cc financing data.



Returns
-----=
<code>DataFrame</code>
:   A Dataframe containing active financing metrics by financing type


and currency.


### Function `calculates_active_financing_metrics_task` {#id}




>     def calculates_active_financing_metrics_task(
>         p_cleaned_cc_extra_financing: pyspark.sql.dataframe.DataFrame,
>         p_cleaned_cc_account: pyspark.sql.dataframe.DataFrame
>     ) ‑> pyspark.sql.dataframe.DataFrame


Aggregates active financing metrics, including the financing count,
earliest and latest financing dates, total pending installments,
and total financing term in months.


Args
-----=
**```p_cleaned_cc_extra_financing```** :&ensp;<code>DataFrame</code>
:   Cleaned cc financing data.


**```p_cleaned_cc_account```** :&ensp;<code>DataFrame</code>
:   Cleaned credit card data



Returns
-----=
<code>DataFrame</code>
:   A Dataframe containing active financing metrics.




### Function `credit_card_flow` {#id}




>     def credit_card_flow()


Loads, transforms and saves main credit card features in the data lake.

The flow performs the following operations:
1. Loads cleaned credit card data using the specified date range.
2. Transforms and processes the credit card features data.
3. Saves the processed data to the appropriate environment using the
   specified overwrite strategy.


Returns
-----=
<code>None</code>
:   This flow does not return a value, but saves the processed


credit card features data.


### Function `process_credit_card_account_task` {#id}




>     def process_credit_card_account_task(
>         p_cleaned_cc_account: pyspark.sql.dataframe.DataFrame,
>         p_cleaned_cc_updates: pyspark.sql.dataframe.DataFrame,
>         p_cleaned_daily_rate: pyspark.sql.dataframe.DataFrame
>     ) ‑> pyspark.sql.dataframe.DataFrame


Transforms and processes credit cards account data.


Args
-----=
**```p_cleaned_cc_account```** :&ensp;<code>DataFrame</code>
:   Cleaned main credit card account data.


**```p_cleaned_cc_updates```** :&ensp;<code>DataFrame</code>
:   Cleaned credit card updates data.


**```p_cleaned_daily_rate```** :&ensp;<code>DataFrame</code>
:   Cleaned daily exchange data.



Returns
-----=
<code>DataFrame</code>
:   Transformed main credit card account DataFrame.




### Function `process_credit_card_balance_task` {#id}




>     def process_credit_card_balance_task(
>         p_cleaned_cc_balance: pyspark.sql.dataframe.DataFrame,
>         p_cleaned_daily_rate: pyspark.sql.dataframe.DataFrame
>     ) ‑> pyspark.sql.dataframe.DataFrame


Transforms and processes credit cards balance data.


Args
-----=
**```p_cleaned_cc_balance```** :&ensp;<code>DataFrame</code>
:   Cleaned credit card balance data.


**```p_cleaned_daily_rate```** :&ensp;<code>DataFrame</code>
:   Cleaned daily exchange data.



Returns
-----=
<code>DataFrame</code>
:   Transformed credit card balance DataFrame.




### Function `process_credit_card_monthly_payments_task` {#id}




>     def process_credit_card_monthly_payments_task(
>         p_cleaned_cc_transaction_universe: pyspark.sql.dataframe.DataFrame,
>         p_cleaned_daily_rate: pyspark.sql.dataframe.DataFrame
>     ) ‑> pyspark.sql.dataframe.DataFrame


Transforms and processes credit cards monthly payments data.


Args
-----=
**```p_cleaned_cc_transaction_universe```** :&ensp;<code>DataFrame</code>
:   Cleaned credit card payments.


**```p_cleaned_daily_rate```** :&ensp;<code>DataFrame</code>
:   Cleaned daily exchange data.



Returns
-----=
<code>DataFrame</code>
:   Transformed credit card payments DataFrame.




### Function `process_credit_card_tenure_tsae83_task` {#id}




>     def process_credit_card_tenure_tsae83_task(
>         p_cleaned_cc_tenure_tsae83: pyspark.sql.dataframe.DataFrame
>     ) ‑> pyspark.sql.dataframe.DataFrame


Transforms and processes credit cards tenure data from tsae83.


Args
-----=
**```p_cleaned_cc_tenure_tsae83```** :&ensp;<code>DataFrame</code>
:   Cleaned tenure from tsae83 data.



Returns
-----=
<code>DataFrame</code>
:   Transformed credit card tsae83 tenure DataFrame.





-----
Generated by *pdoc* 0.11.1 (<https://pdoc3.github.io>).
