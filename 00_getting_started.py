# Databricks notebook source
# MAGIC %md
# MAGIC  
# MAGIC # Lung Cancer Survival Analysis Based on RWD
# MAGIC  
# MAGIC In this notebook we showcase an example of performing survival analysis using `python` and `R` on longitudinal synthetic patient records.
# MAGIC The main objective of this solution accelerator is to highlight `a)` Leveraging lakehouse paradigm for statistical analysis in life sciences.
# MAGIC `b)` Using simulated data as the ground truth for validation of workloads.
# MAGIC ## Data
# MAGIC We use simulated data for `~100K` patients to compare survival of patients diagnosed with Small Cell Carcinoma of Lung (SCLC) and Non Small Cell Lung Cancer (NSCLC). To this end, we create a database of patient records that includes `encounters`, `patients` and `conditions` tables. Using these data we create a cohort of lung cancer patients.
# MAGIC Data are generated with [synthea](https://github.com/synthetichealth/synthea/wiki) using [lung cancer](https://synthetichealth.github.io/module-builder/#lung_cancer) disease module.
# MAGIC We then use [survival](https://cran.r-project.org/web/packages/survival/index.html) package in R to perform [Kaplan-Meier survival analysis](https://en.wikipedia.org/wiki/Kaplan%E2%80%93Meier_estimator).
# MAGIC The following diagram is snapshot of the
# MAGIC logic used for simulating the data:
# MAGIC
# MAGIC <a href="https://synthetichealth.github.io/module-builder/#lung_cancer" style="width:100px;height:100px;"><img src="https://hls-eng-data-public.s3.amazonaws.com/img/lung_cancer_module.gif"></a>
# MAGIC  
# MAGIC ## Dataflow
# MAGIC The following diagram summarized the dataflow in this notebook:
# MAGIC  
# MAGIC [![](https://mermaid.ink/img/pako:eNp1UktuwjAQvcrIa7hAFpUCBGhVsSjsEhRMPAFXzhj5g0qBW_RcPVNNHCQKrVfzee_Nx3NklRbIErYxfLeFxaggCM_6dQxY2XjFHcKOO4nkwGCljbARlqa5PZDbIk9AedqUFacKDTRaeIXLCEIScKf6_QVro-kTrzLQ7z9BmndF7PJXfJBXmoR0UtNdZpgjVdqTQ2Mfqg1ayKmWKqST-fB1CNrA7GKcIMtv-i3v6g4js-EfSTYblaN0kZ1gnCtuXSnCMkpNZdxDRxi3hMnxXUtKLmKErpTi3DUbs8GK_qT1p392UIq608wi63FzVqo9mhifwv3U05b2nFtv9nLPFaTE1cFKC95K2sDqmliBJHhb3rJe_mcpWaMKY9mWtju4rabAZT3WoGm4FOGEjhetgoVzaLBgSTAF1twrV7CCzgHqd5ftZeErtWFJzZXFHuPe6fmBKpY44_EKGkkeBm461PkHRPzeSw)](https://mermaid-js.github.io/mermaid-live-editor/edit/#pako:eNp1UktuwjAQvcrIa7hAFpUCBGhVsSjsEhRMPAFXzhj5g0qBW_RcPVNNHCQKrVfzee_Nx3NklRbIErYxfLeFxaggCM_6dQxY2XjFHcKOO4nkwGCljbARlqa5PZDbIk9AedqUFacKDTRaeIXLCEIScKf6_QVro-kTrzLQ7z9BmndF7PJXfJBXmoR0UtNdZpgjVdqTQ2Mfqg1ayKmWKqST-fB1CNrA7GKcIMtv-i3v6g4js-EfSTYblaN0kZ1gnCtuXSnCMkpNZdxDRxi3hMnxXUtKLmKErpTi3DUbs8GK_qT1p392UIq608wi63FzVqo9mhifwv3U05b2nFtv9nLPFaTE1cFKC95K2sDqmliBJHhb3rJe_mcpWaMKY9mWtju4rabAZT3WoGm4FOGEjhetgoVzaLBgSTAF1twrV7CCzgHqd5ftZeErtWFJzZXFHuPe6fmBKpY44_EKGkkeBm461PkHRPzeSw)
