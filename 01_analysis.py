# Databricks notebook source
# MAGIC %md
# MAGIC # Lung Cancer Survival Analysis
# MAGIC In this notebook we demonstrate how to use databricks platform to
# MAGIC
# MAGIC 1. Load patient's records into lakehouse
# MAGIC 2. Use SQL to manipulate the data and prepare for your inference step
# MAGIC 3. Use R and Python for survival analysis

# COMMAND ----------

# MAGIC %md
# MAGIC # 0. Configuration

# COMMAND ----------

# MAGIC %python
# MAGIC from pyspark.sql import functions as F
# MAGIC from pyspark.sql import Window
# MAGIC import re
# MAGIC import json

# COMMAND ----------

# DBTITLE 1,define paths
# MAGIC %python
# MAGIC project_name = 'lung-cancer-survival-analysis'
# MAGIC source_data_path='s3://hls-eng-data-public/data/synthea/lung_cancer/csv/'
# MAGIC target_data_path=f'/FileStore/{project_name}'
# MAGIC  
# MAGIC db_name='synthea_survival_demo'
# MAGIC display(dbutils.fs.ls(source_data_path))

# COMMAND ----------

# MAGIC %md # 1. Data Ingest and Exploration
# MAGIC ingesting data as spark dataframe and storing in bronze stage

# COMMAND ----------

# DBTITLE 1,Ingest csv files and write to delta bronze layer
# MAGIC %python
# MAGIC from concurrent.futures import ThreadPoolExecutor
# MAGIC from collections import deque
# MAGIC #define the datasets needed for the analysis
# MAGIC datasets = ['patients','conditions','encounters']

# COMMAND ----------

# create a database
spark.sql(f"""create database if not exists {db_name} LOCATION '{target_data_path}'""")
spark.sql(f"""USE {db_name}""")

# COMMAND ----------

#define a function for loading raw data and saving as sql tables
def load_folder_as_table(dataset):
  print(f'loading {source_data_path}/{dataset} as a delta table {dataset}...')
  (
    spark.read.csv(f'{source_data_path}/{dataset}.csv.gz',header=True,inferSchema=True)
    .write.format("delta").mode("overwrite").saveAsTable(f'{dataset}')
  )

# COMMAND ----------

#note: we speed up a little bit the ingestion starting 3 tables at a time with a ThreadPoolExecutor
with ThreadPoolExecutor(max_workers=3) as executor:
    deque(executor.map(load_folder_as_table, datasets))

# COMMAND ----------

# DBTITLE 1,count of records
import pandas as pd
table_counts=[(tab,sql(f'select * from {tab}').count()) for tab in datasets]
display(pd.DataFrame(table_counts,columns=['dataset','n_records']).sort_values(by=['n_records'],ascending=False))

# COMMAND ----------

# DBTITLE 1,Patient table
# MAGIC %sql
# MAGIC select * from patients
# MAGIC limit 20

# COMMAND ----------

# DBTITLE 1,Conditions
# MAGIC %sql
# MAGIC select * from conditions
# MAGIC limit 20

# COMMAND ----------

# DBTITLE 1,Encounters
# MAGIC %sql
# MAGIC select * from encounters
# MAGIC limit 20

# COMMAND ----------

# MAGIC %md
# MAGIC Now, let's take a look at the distribution of cell types. SNOWMED codes for small cell and none-small cell lung cancer are `SCLC:254632001`, `NSCLC:254637007`.

# COMMAND ----------

# DBTITLE 1,distribution of cell type
# MAGIC %sql
# MAGIC select CODE, DESCRIPTION, count('*') as cnt
# MAGIC from conditions
# MAGIC where code in (254632001,254637007)
# MAGIC group by 1,2

# COMMAND ----------

# MAGIC %md
# MAGIC We can also look into the distribution of codes based on both cell type and the stage of diagnosis (I,II,III and IV)

# COMMAND ----------

codes={'SCLC_IV': 67841000119103,
'NSCLC_IV':423121009,
'SCLC_III':67831000119107,
'NSCLC_III':422968005,
'SCLC_II':67821000119109,
'NSCLC_II':425048006,
'SCLC_I':67811000119102,
'NSCLC_I':424132000
}

# COMMAND ----------

# MAGIC %sql
# MAGIC select CODE, DESCRIPTION, count('*') as cnt
# MAGIC from conditions
# MAGIC where code in (67841000119103,423121009,67831000119107,422968005,67821000119109,425048006,67811000119102,424132000)
# MAGIC group by 1,2

# COMMAND ----------

# MAGIC %md As we see, in this dataset almost all patients are diagnosed with stage I cancer and only one patient is diagnosed with `NSCC II`.

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Data Transformation
# MAGIC Now we create a cohort of all paitents that have been diagnosed with lung cancer. To create this cohort, we use the `conditions` table.

# COMMAND ----------

# DBTITLE 1,patients diagnosed with lung cancer
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW lung_cancer_cohort 
# MAGIC     AS (
# MAGIC         select PATIENT, to_date(START) as START_DATE, 
# MAGIC             CASE
# MAGIC                 WHEN CODE==254632001 THEN 'SCLC'
# MAGIC                 ELSE 'NSCLC'
# MAGIC             END as type
# MAGIC         from conditions
# MAGIC         where code in (254632001,254637007)
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM lung_cancer_cohort limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC Note: It would be recommended to add the resulting cohort to a `results` schema within your silver layer.

# COMMAND ----------

# MAGIC %md
# MAGIC Now we proceed to create the final dataset that will be used in our analysis. To do so, we need to get the last date on the record for each patient (based on the `encounter` table), and join the data with patient demogrpahic information (gender at birth, date of birth and date of death), and finally join the data with the cohort table.

# COMMAND ----------

# DBTITLE 1,create lung cancer patients dataset
# MAGIC %sql
# MAGIC CREATE or REPLACE TEMP VIEW lung_cancer_patients_dataset AS (
# MAGIC     with last_date_on_record AS (
# MAGIC         select PATIENT, max(to_date(STOP)) as last_date_on_record from encounters
# MAGIC         group by PATIENT
# MAGIC     )
# MAGIC     ,
# MAGIC     patients_and_dates AS (
# MAGIC         select Id, to_date(BIRTHDATE) as BIRTHDATE, to_date(DEATHDATE) death_date, GENDER, ld.last_date_on_record
# MAGIC         from patients
# MAGIC         join last_date_on_record ld
# MAGIC             on ld.PATIENT == patients.Id
# MAGIC     )
# MAGIC     
# MAGIC     SELECT *, round(datediff(START_DATE,BIRTHDATE)/356) as age_at_diagnosis from lung_cancer_cohort lcc
# MAGIC     join patients_and_dates pad 
# MAGIC     on lcc.PATIENT= pad.Id 
# MAGIC )

# COMMAND ----------

# DBTITLE 1,Lung Cancer Patient Data
# MAGIC %sql
# MAGIC select * from lung_cancer_patients_dataset
# MAGIC limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view age_at_diagnosis_view as
# MAGIC select age_at_diagnosis, GENDER, type 
# MAGIC from lung_cancer_patients_dataset

# COMMAND ----------

# DBTITLE 1,Age Distibution
import plotly.express as px
_pdf = spark.table("age_at_diagnosis_view").toPandas()

# COMMAND ----------

px.histogram(_pdf,x='age_at_diagnosis',color='GENDER',pattern_shape="type", marginal="box", hover_data=_pdf.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC We see that in our dataset, the median age of female patients is `~57` whearas for males it is much higher at `~66` years.

# COMMAND ----------

# DBTITLE 1,Save the final dataset
sql("select * from lung_cancer_patients_dataset").write.mode("overWrite").save(f'{target_data_path}/silver/lung-cancer-patients-dataset')

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Survival Analysis
# MAGIC Now, we are ready to perform survival anlysis on the dataset provided. Without going into details of survival analysis, we provide an overview of performing an standard survival anlalysis in Python on databricks lakehouse platform. 

# COMMAND ----------

# MAGIC %md ### Data Preperation
# MAGIC First, we need to create a dataset of patients and their survival time after the condition onset (first diagnosis) with SCLC or NSCLC.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW lung_cancer_survival_data AS (
# MAGIC     SELECT 
# MAGIC         START_DATE,
# MAGIC         death_date,
# MAGIC         GENDER,
# MAGIC         type,
# MAGIC         age_at_diagnosis,
# MAGIC         CASE WHEN death_date is null THEN 0 ELSE 1 END as status,
# MAGIC         CASE WHEN death_date is null THEN datediff(last_date_on_record,START_DATE) ELSE datediff(death_date,START_DATE) END as time
# MAGIC FROM lung_cancer_patients_dataset
# MAGIC )

# COMMAND ----------

# DBTITLE 1,Survival time data
# MAGIC %sql
# MAGIC SELECT * from lung_cancer_survival_data
# MAGIC limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC Analysis in python using the [lifeline](https://pypi.org/project/lifelines/) package

# COMMAND ----------

# MAGIC %pip install lifelines

# COMMAND ----------

import pandas as pd
from lifelines import KaplanMeierFitter

# COMMAND ----------

# MAGIC %python
# MAGIC data_df = sql('select * from lung_cancer_survival_data')
# MAGIC data_pdf = data_df.toPandas()

# COMMAND ----------

# MAGIC %python
# MAGIC T = data_pdf['time']
# MAGIC E = data_pdf['status']

# COMMAND ----------

# MAGIC %md
# MAGIC where `T` is the duration, `E` can either be a boolean or binary array representing whether the “death” was observed or not. Now, we will fit a Kaplan Meier model to the data.

# COMMAND ----------

kmf = KaplanMeierFitter()
kmf.fit(T, E)

# COMMAND ----------

kmf.survival_function_
kmf.cumulative_density_
kmf.plot_survival_function()

# COMMAND ----------

# MAGIC %md Alternatively, you can plot the cumulative density function:

# COMMAND ----------

kmf.plot_cumulative_density()

# COMMAND ----------


