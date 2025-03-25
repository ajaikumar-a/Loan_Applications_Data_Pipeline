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
