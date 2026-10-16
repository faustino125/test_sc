---
description: |
    API documentation for modules: data_engineering.data_cleaning.src.credit_card, data_engineering.data_cleaning.src.credit_card.cc_account_flow, data_engineering.data_cleaning.src.credit_card.cc_additional_account_flow, data_engineering.data_cleaning.src.credit_card.cc_balance_flow, data_engineering.data_cleaning.src.credit_card.cc_categories_flow, data_engineering.data_cleaning.src.credit_card.cc_customer_extra_financing_flow, data_engineering.data_cleaning.src.credit_card.cc_extra_financing_master_flow, data_engineering.data_cleaning.src.credit_card.cc_tenure_tsae83_flow, data_engineering.data_cleaning.src.credit_card.cc_transaction_universe_flow, data_engineering.data_cleaning.src.credit_card.cc_updates_flow.

lang: en

classoption: oneside
geometry: margin=1in
papersize: a4

linkcolor: blue
links-as-notes: true
...



# Module `data_engineering.data_cleaning.src.credit_card` {#id}





## Sub-modules

* [data_engineering.data_cleaning.src.credit_card.cc_account_flow](#data_engineering.data_cleaning.src.credit_card.cc_account_flow)
* [data_engineering.data_cleaning.src.credit_card.cc_additional_account_flow](#data_engineering.data_cleaning.src.credit_card.cc_additional_account_flow)
* [data_engineering.data_cleaning.src.credit_card.cc_balance_flow](#data_engineering.data_cleaning.src.credit_card.cc_balance_flow)
* [data_engineering.data_cleaning.src.credit_card.cc_categories_flow](#data_engineering.data_cleaning.src.credit_card.cc_categories_flow)
* [data_engineering.data_cleaning.src.credit_card.cc_customer_extra_financing_flow](#data_engineering.data_cleaning.src.credit_card.cc_customer_extra_financing_flow)
* [data_engineering.data_cleaning.src.credit_card.cc_extra_financing_master_flow](#data_engineering.data_cleaning.src.credit_card.cc_extra_financing_master_flow)
* [data_engineering.data_cleaning.src.credit_card.cc_tenure_tsae83_flow](#data_engineering.data_cleaning.src.credit_card.cc_tenure_tsae83_flow)
* [data_engineering.data_cleaning.src.credit_card.cc_transaction_universe_flow](#data_engineering.data_cleaning.src.credit_card.cc_transaction_universe_flow)
* [data_engineering.data_cleaning.src.credit_card.cc_updates_flow](#data_engineering.data_cleaning.src.credit_card.cc_updates_flow)







# Module `data_engineering.data_cleaning.src.credit_card.cc_account_flow` {#id}







## Functions



### Function `cc_account_flow` {#id}




>     def cc_account_flow()


Load, process, and save credit card accounts data in the data lake.

This flow performs the following operations:
1. Loads raw credit card accounts data using the specified date range.
2. Cleans and processes the credit card accounts data.
3. Saves the processed data to the appropriate environment using the
   specified overwrite strategy.


Returns
-----=
<code>None</code>
:   This flow does not return a value, but saves the processed credit


card accounts data.


### Function `get_nonrenewal_flag_task` {#id}




>     def get_nonrenewal_flag_task(
>         p_non_renewal_col: str,
>         p_field_alias: str,
>         p_upgrade_flag=False
>     ) ‑> pyspark.sql.dataframe.DataFrame


Flag for labeling non renewal customers.


Args
-----=
**```p_non_renewal_col```** :&ensp;<code>Column</code>
:   column used to create flag for p_non_renewal_col.


**```p_field_alias```** :&ensp;<code>str</code>
:   alias for new field.


**```p_upgrade_flag```** :&ensp;<code>bool</code>
:   upgrades column flag.



Returns
-----=
<code>Column</code>
:   column with non_renewal_flag




### Function `get_situation_group_task` {#id}




>     def get_situation_group_task(
>         p_orig_situation_desc: pyspark.sql.column.Column,
>         p_col_alias: str
>     ) ‑> pyspark.sql.column.Column


Using the original situation account data,
return the situation account group


Args
-----=
**```p_orig_situation_desc```** :&ensp;<code>Column</code>
:   The column to be validated


**```p_field_alias```** :&ensp;<code>str</code>
:   Name for the output Column



Returns
-----=
<code>Column</code>
:   Column with situation group description.




### Function `payment_date_task` {#id}




>     def payment_date_task(
>         p_statement_date: pyspark.sql.column.Column,
>         p_card_type: pyspark.sql.column.Column
>     )


Return the payment date based on the statement date


Args
-----=
**```p_statement_date```** :&ensp;<code>Column</code>
:   credit card statement date


**```p_card_type```** :&ensp;<code>Column</code>
:   credit card type



Returns
-----=
<code>Column</code>
:   Payment date column







# Module `data_engineering.data_cleaning.src.credit_card.cc_additional_account_flow` {#id}







## Functions



### Function `cc_additional_account_flow` {#id}




>     def cc_additional_account_flow()


Load, process, and save additional credit card accounts data in the data lake.

This flow performs the following operations:
1. Loads raw additional credit card accounts data using the specified date range.
2. Cleans and processes the additional credit card accounts data.
3. Saves the processed data to the appropriate environment using the
   specified overwrite strategy.


Returns
-----=
<code>None</code>
:   This flow does not return a value, but saves the processed additional


credit card accounts data.





# Module `data_engineering.data_cleaning.src.credit_card.cc_balance_flow` {#id}







## Functions



### Function `cc_balance_flow` {#id}




>     def cc_balance_flow()


Load, process, and save credit card balance data in the data lake.

This flow performs the following operations:
1. Loads raw credit card balance data using the specified date range.
2. Cleans and processes the credit card balance data.
3. Saves the processed data to the appropriate environment using the
   specified overwrite strategy.


Returns
-----=
<code>None</code>
:   This flow does not return a value, but saves the processed credit


card balance data.





# Module `data_engineering.data_cleaning.src.credit_card.cc_categories_flow` {#id}







## Functions



### Function `cc_categories_flow` {#id}




>     def cc_categories_flow()


Load, process, and save credit card categories data in the data lake.

This flow performs the following operations:
1. Loads raw credit card categories data using the specified date range.
2. Cleans and processes the credit card categories data.
3. Saves the processed data to the appropriate environment using the
   specified overwrite strategy.


Returns
-----=
<code>None</code>
:   This flow does not return a value, but saves the processed credit


card categories data.


### Function `cc_category_task` {#id}




>     def cc_category_task(
>         p_cc_cat: pyspark.sql.column.Column,
>         p_dict,
>         p_cc_brand=None
>     ) ‑> pyspark.sql.column.Column


Get the category of a credit card.


Args
-----=
**```p_cc_cat```**
:   column with raw cc category


**```p_dict```**
:   dictionary to compare


**```p_cc_brand```**
:   brand column



Returns
-----=
<code>Column</code>
:   column with id_category, category or next_category







# Module `data_engineering.data_cleaning.src.credit_card.cc_customer_extra_financing_flow` {#id}







## Functions



### Function `cc_customer_extra_financing_flow` {#id}




>     def cc_customer_extra_financing_flow()


Load, process, and save customer extra financing data in the data lake.

This flow performs the following operations:
1. Loads raw customer extra financing data using the specified date range.
2. Cleans and processes customer extra financing data.
3. Saves the processed data to the appropriate environment using the
   specified overwrite strategy.


Returns
-----=
<code>None</code>
:   This flow does not return a value, but saves the processed data.




### Function `is_eligible_for_financing_task` {#id}




>     def is_eligible_for_financing_task(
>         p_available_amount: pyspark.sql.column.Column,
>         p_available_installment_amount: pyspark.sql.column.Column,
>         p_limit_amount: pyspark.sql.column.Column
>     )


Evaluates the customer's eligibility for financing


Args
-----=
**```p_available_amount```** :&ensp;<code>col</code>
:   available_amount


p_available_installment_amount(col): available_installment_amount
p_limit_amount(col): limit

Returns
-----=
<code>Column</code>
:   is_eligible_for_financing







# Module `data_engineering.data_cleaning.src.credit_card.cc_extra_financing_master_flow` {#id}







## Functions



### Function `cc_extra_financing_master_flow` {#id}




>     def cc_extra_financing_master_flow()


Load, process, and save credit card extra financing data in the data lake.

This flow performs the following operations:
1. Loads raw credit card extra financing data using the specified date range.
2. Cleans and processes the credit card extra financing data.
3. Saves the processed data to the appropriate environment using the
   specified overwrite strategy.


Returns
-----=
<code>None</code>
:   This flow does not return a value, but saves the processed data.







# Module `data_engineering.data_cleaning.src.credit_card.cc_tenure_tsae83_flow` {#id}







## Functions



### Function `cc_tenure_tsae83_flow` {#id}




>     def cc_tenure_tsae83_flow()


Load, process, and save credit card tenure tsae83 data in the data lake.

This flow performs the following operations:
1. Loads raw credit card tenure tsae83 data using the specified date range.
2. Cleans and processes the credit card tenure tsae83 data.
3. Saves the processed data to the appropriate environment using the
   specified overwrite strategy.


Returns
-----=
<code>None</code>
:   This flow does not return a value, but saves the processed credit


card tenure tsae83 data.





# Module `data_engineering.data_cleaning.src.credit_card.cc_transaction_universe_flow` {#id}







## Functions



### Function `cc_transaction_universe_flow` {#id}




>     def cc_transaction_universe_flow()


Load, process, and save credit card transaction universe data in the data lake.

This flow performs the following operations:
1. Loads raw credit card transaction universe data using the specified date range.
2. Cleans and processes the credit card transaction universe data.
3. Saves the processed data to the appropriate environment using the
   specified overwrite strategy.


Returns
-----=
<code>None</code>
:   This flow does not return a value, but saves the processed credit


card transaction universe data.





# Module `data_engineering.data_cleaning.src.credit_card.cc_updates_flow` {#id}







## Functions



### Function `cc_updates_flow` {#id}




>     def cc_updates_flow()


Load, process, and save credit card updates data in the data lake.

This flow performs the following operations:
1. Loads raw credit card updates data using the specified date range.
2. Cleans and processes the credit card updates data.
3. Saves the processed data to the appropriate environment using the
   specified overwrite strategy.


Returns
-----=
<code>None</code>
:   This flow does not return a value, but saves the processed credit


card updates data.



-----
Generated by *pdoc* 0.11.1 (<https://pdoc3.github.io>).
