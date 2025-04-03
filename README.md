## Loan Applications Data Pipeline Project

### About the project:
In this project, I'll be setting up an ETL pipeline using Pyspark with the Loan Applications dataset.

Steps involved:
1. Data ingestion
2. Data cleaning
3. Data transformation
4. Unit testing
5. Storing processed data to database table


#### 1. Data Ingestion
I've used the Loan Default Data from Kaggle for this project.

Data source: https://www.kaggle.com/datasets/gauravduttakiit/loan-defaulter

#### 2. Data Cleaning
Data cleaning activities performed:
- Removed duplicate records
- Dropped unnecessary fields
- Round numeric values

#### 3. Data Transformation
Data transformations performed: 
- Created calculated fields
- Created summary dataframes
- Saved the processed dataframe as Parquet files

#### 4. Database Setup
For this project, I've used Postgres database to store the processed data. Following is the table schema details: 

```
create table loan_applications(
SK_ID_CURR integer, 
NAME_CONTRACT_TYPE varchar(100), 
CODE_GENDER varchar(100), 
FLAG_OWN_CAR varchar(100), 
FLAG_OWN_REALTY varchar(100), 
CNT_CHILDREN integer, 
AMT_INCOME_TOTAL float, 
AMT_CREDIT float, 
AMT_ANNUITY float, 
AMT_GOODS_PRICE float, 
NAME_TYPE_SUITE varchar(100), 
NAME_INCOME_TYPE varchar(100), 
NAME_EDUCATION_TYPE varchar(100), 
NAME_FAMILY_STATUS varchar(100), 
NAME_HOUSING_TYPE varchar(100), 
REGION_POPULATION_RELATIVE float, 
OWN_CAR_AGE float, 
FLAG_MOBIL integer, 
FLAG_EMP_PHONE integer, 
FLAG_WORK_PHONE integer, 
FLAG_CONT_MOBILE integer, 
FLAG_PHONE integer, 
FLAG_EMAIL integer, 
OCCUPATION_TYPE varchar(100), 
CNT_FAM_MEMBERS float, 
REGION_RATING_CLIENT integer, 
REGION_RATING_CLIENT_W_CITY integer, 
WEEKDAY_APPR_PROCESS_START varchar(100), 
HOUR_APPR_PROCESS_START integer, 
REG_REGION_NOT_LIVE_REGION integer, 
REG_REGION_NOT_WORK_REGION integer, 
LIVE_REGION_NOT_WORK_REGION integer, 
REG_CITY_NOT_LIVE_CITY integer, 
REG_CITY_NOT_WORK_CITY integer, 
LIVE_CITY_NOT_WORK_CITY integer, 
ORGANIZATION_TYPE varchar(100), 
APARTMENTS_AVG float, 
BASEMENTAREA_AVG float, 
YEARS_BEGINEXPLUATATION_AVG float, 
YEARS_BUILD_AVG float, 
COMMONAREA_AVG float, 
ELEVATORS_AVG float, 
ENTRANCES_AVG float, 
FLOORSMAX_AVG float, 
FLOORSMIN_AVG float, 
LANDAREA_AVG float, 
LIVINGAPARTMENTS_AVG float, 
LIVINGAREA_AVG float, 
NONLIVINGAPARTMENTS_AVG float, 
NONLIVINGAREA_AVG float, 
APARTMENTS_MODE float, 
BASEMENTAREA_MODE float, 
YEARS_BEGINEXPLUATATION_MODE float, 
YEARS_BUILD_MODE float, 
COMMONAREA_MODE float, 
ELEVATORS_MODE float, 
ENTRANCES_MODE float, 
FLOORSMAX_MODE float, 
FLOORSMIN_MODE float, 
LANDAREA_MODE float, 
LIVINGAPARTMENTS_MODE float, 
LIVINGAREA_MODE float, 
NONLIVINGAPARTMENTS_MODE float, 
NONLIVINGAREA_MODE float, 
APARTMENTS_MEDI float, 
BASEMENTAREA_MEDI float, 
YEARS_BEGINEXPLUATATION_MEDI float, 
YEARS_BUILD_MEDI float, 
COMMONAREA_MEDI float, 
ELEVATORS_MEDI float, 
ENTRANCES_MEDI float, 
FLOORSMAX_MEDI float, 
FLOORSMIN_MEDI float, 
LANDAREA_MEDI float, 
LIVINGAPARTMENTS_MEDI float, 
LIVINGAREA_MEDI float, 
NONLIVINGAPARTMENTS_MEDI float, 
NONLIVINGAREA_MEDI float, 
FONDKAPREMONT_MODE varchar(100), 
HOUSETYPE_MODE varchar(100), 
TOTALAREA_MODE float, 
WALLSMATERIAL_MODE varchar(100), 
EMERGENCYSTATE_MODE varchar(100), 
OBS_30_CNT_SOCIAL_CIRCLE float, 
DEF_30_CNT_SOCIAL_CIRCLE float, 
OBS_60_CNT_SOCIAL_CIRCLE float, 
DEF_60_CNT_SOCIAL_CIRCLE float, 
FLAG_DOCUMENT_2 integer, 
FLAG_DOCUMENT_3 integer, 
FLAG_DOCUMENT_4 integer, 
FLAG_DOCUMENT_5 integer, 
FLAG_DOCUMENT_6 integer, 
FLAG_DOCUMENT_7 integer, 
FLAG_DOCUMENT_8 integer, 
FLAG_DOCUMENT_9 integer, 
FLAG_DOCUMENT_10 integer, 
FLAG_DOCUMENT_11 integer, 
FLAG_DOCUMENT_12 integer, 
FLAG_DOCUMENT_13 integer, 
FLAG_DOCUMENT_14 integer, 
FLAG_DOCUMENT_15 integer, 
FLAG_DOCUMENT_16 integer, 
FLAG_DOCUMENT_17 integer, 
FLAG_DOCUMENT_18 integer, 
FLAG_DOCUMENT_19 integer, 
FLAG_DOCUMENT_20 integer, 
FLAG_DOCUMENT_21 integer, 
AMT_REQ_CREDIT_BUREAU_HOUR float, 
AMT_REQ_CREDIT_BUREAU_DAY float, 
AMT_REQ_CREDIT_BUREAU_WEEK float, 
AMT_REQ_CREDIT_BUREAU_MON float, 
AMT_REQ_CREDIT_BUREAU_QRT float, 
AMT_REQ_CREDIT_BUREAU_YEAR float, 
DEBT_TO_INCOME_RATIO float, 
LTV float, 
INCOME_CATEGORY varchar(100)
);
```

#### 5. Load data to database table
The cleaned PySpark dataframe is loaded to the Postgres table.

