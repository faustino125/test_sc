---
description: |
    API documentation for modules: data_engineering.data_cleaning.src.equifax_loan, data_engineering.data_cleaning.src.equifax_loan.eql_agencies_debt_detail_flow, data_engineering.data_cleaning.src.equifax_loan.eql_external_bureau_flow, data_engineering.data_cleaning.src.equifax_loan.eql_person_flow, data_engineering.data_cleaning.src.equifax_loan.eql_person_request_flow, data_engineering.data_cleaning.src.equifax_loan.eql_request_flow, data_engineering.data_cleaning.src.equifax_loan.eql_sib_debt_detail_flow, data_engineering.data_cleaning.src.equifax_loan.eql_sib_debt_history_flow.

lang: en

classoption: oneside
geometry: margin=1in
papersize: a4

linkcolor: blue
links-as-notes: true
...



# Module `data_engineering.data_cleaning.src.equifax_loan` {#id}





## Sub-modules

* [data_engineering.data_cleaning.src.equifax_loan.eql_agencies_debt_detail_flow](#data_engineering.data_cleaning.src.equifax_loan.eql_agencies_debt_detail_flow)
* [data_engineering.data_cleaning.src.equifax_loan.eql_external_bureau_flow](#data_engineering.data_cleaning.src.equifax_loan.eql_external_bureau_flow)
* [data_engineering.data_cleaning.src.equifax_loan.eql_person_flow](#data_engineering.data_cleaning.src.equifax_loan.eql_person_flow)
* [data_engineering.data_cleaning.src.equifax_loan.eql_person_request_flow](#data_engineering.data_cleaning.src.equifax_loan.eql_person_request_flow)
* [data_engineering.data_cleaning.src.equifax_loan.eql_request_flow](#data_engineering.data_cleaning.src.equifax_loan.eql_request_flow)
* [data_engineering.data_cleaning.src.equifax_loan.eql_sib_debt_detail_flow](#data_engineering.data_cleaning.src.equifax_loan.eql_sib_debt_detail_flow)
* [data_engineering.data_cleaning.src.equifax_loan.eql_sib_debt_history_flow](#data_engineering.data_cleaning.src.equifax_loan.eql_sib_debt_history_flow)







# Module `data_engineering.data_cleaning.src.equifax_loan.eql_agencies_debt_detail_flow` {#id}







## Functions



### Function `asset_type_aggrupation_task` {#id}




>     def asset_type_aggrupation_task(
>         p_active_type: pyspark.sql.column.Column
>     ) ‑> pyspark.sql.column.Column


Builts, based on the asset type descriptions, a classification that will be
used to build aggrupations later on.

Args
-----=
**```p_active_type```** :&ensp;<code>Column</code>
:   Active type descriptions.



Returns
-----=
<code>Column</code>
:   Active type aggrupation.




### Function `asset_type_task` {#id}




>     def asset_type_task(
>         p_reference1_type: pyspark.sql.column.Column,
>         p_reference2_type: pyspark.sql.column.Column,
>         p_reference3_type: pyspark.sql.column.Column
>     ) ‑> pyspark.sql.column.Column


Identify the field that contains the asset type

Args
-----=
**```p_reference1_type```**
:   Type of reference


**```p_reference2_type```**
:   Type of reference


**```p_reference3_type```**
:   Type of reference



Returns
-----=
<code>Column</code>
:   Active type descriptions.




### Function `eql_agencies_debt_detail_flow` {#id}




>     def eql_agencies_debt_detail_flow()


Load, process, and save agencies debt detail transactional data into the data lake.

The flow performs the following operations:
1. Loads agencies debt detail transactional raw data using the specified date range.
2. Cleans and processes the agencies debt detail transactional data.
3. Saves the processed data to the appropriate environment using the
   specified overwrite strategy.


Returns
-----=
<code>None</code>
:   This flow does not return a value, but saves the processed


agencies debt detail transactional data.


### Function `map_status_task` {#id}




>     def map_status_task(
>         p_id_classification_type: pyspark.sql.column.Column
>     ) ‑> pyspark.sql.column.Column


Retrives the current status of the specified entity


Args
-----=
**```p_classification_type```** :&ensp;<code>Column</code>
:   classification type.



Returns
-----=
<code>Column</code>
:   classification type aggrupations.




### Function `past_due_flag_task` {#id}




>     def past_due_flag_task(
>         p_id_classification_type: pyspark.sql.column.Column
>     ) ‑> pyspark.sql.column.Column


build past due flag


Args
-----=
**```p_id_classification_type```** :&ensp;<code>Column</code>
:   classification type.



Returns
-----=
<code>Column</code>
:   past due flag.







# Module `data_engineering.data_cleaning.src.equifax_loan.eql_external_bureau_flow` {#id}







## Functions



### Function `eql_external_bureau_flow` {#id}




>     def eql_external_bureau_flow()


Load, process, and save external bureau data in the data lake.

The flow performs the following operations:
1. Loads external bureau raw data using the specified date range.
2. Cleans and processes external bureau data.
3. Saves the processed data to the appropriate environment using the
   specified overwrite strategy.


Returns
-----=
<code>None</code>
:   This flow does not return a value, but saves the processed


 external bureau data.





# Module `data_engineering.data_cleaning.src.equifax_loan.eql_person_flow` {#id}







## Functions



### Function `eql_person_flow` {#id}




>     def eql_person_flow()


Load, process, and save person data into the data lake.

The flow performs the following operations:
1. Loads person raw data.
2. Cleans and processes person full_load data.
3. Saves the processed data to the appropriate environment using the
   specified overwrite strategy.


Returns
-----=
<code>None</code>
:   This flow does not return a value, but saves the processed


person data.





# Module `data_engineering.data_cleaning.src.equifax_loan.eql_person_request_flow` {#id}







## Functions



### Function `eql_person_request_flow` {#id}




>     def eql_person_request_flow()


Load, process, and save person request data into the data lake.

The flow performs the following operations:
1. Loads person request raw data.
2. Cleans and processes person request full_load data.
3. Saves the processed data to the appropriate environment using the
   specified overwrite strategy.


Returns
-----=
<code>None</code>
:   This flow does not return a value, but saves the processed


person request data.





# Module `data_engineering.data_cleaning.src.equifax_loan.eql_request_flow` {#id}







## Functions



### Function `eql_request_flow` {#id}




>     def eql_request_flow()


Load, process, and save request data into the data lake.

The flow performs the following operations:
1. Loads request raw data.
2. Cleans and processes request full_load data.
3. Saves the processed data to the appropriate environment using the
   specified overwrite strategy.


Returns
-----=
<code>None</code>
:   This flow does not return a value, but saves the processed


request data.





# Module `data_engineering.data_cleaning.src.equifax_loan.eql_sib_debt_detail_flow` {#id}







## Functions



### Function `eql_sib_debt_detail_flow` {#id}




>     def eql_sib_debt_detail_flow()


Load, process, and save SIB debt detail transactional data into the data lake.

The flow performs the following operations:
1. Loads SIB debt detail transactional raw data using the specified date range.
2. Cleans and processes the SIB debt detail transactional data.
3. Saves the processed data to the appropriate environment using the
   specified overwrite strategy.


Returns
-----=
<code>None</code>
:   This flow does not return a value, but saves the processed


SIB debt detail transactional data.





# Module `data_engineering.data_cleaning.src.equifax_loan.eql_sib_debt_history_flow` {#id}







## Functions



### Function `eql_sib_debt_history_flow` {#id}




>     def eql_sib_debt_history_flow()


Load, process, and save debt history transactional data into the data lake.

The flow performs the following operations:
1. Loads debt history transactional raw data using the specified date range.
2. Cleans and processes the debt history transactional data.
3. Saves the processed data to the appropriate environment using the
   specified overwrite strategy.


Returns
-----=
<code>None</code>
:   This flow does not return a value, but saves the processed


debt history transactional data.



-----
Generated by *pdoc* 0.11.5 (<https://pdoc3.github.io>).
