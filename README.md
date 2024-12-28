# Uber-ETL-Pipeline
ETL Pipeline with Apache Airflow on Google Cloud Composer
This project demonstrates the implementation of an end-to-end ETL pipeline on Google Cloud Platform using Apache Airflow and Google Cloud Composer.

# Workflow Overview

Data Ingestion:
1. Raw data is uploaded to a public URL.
2. The data is fetched using GCP hooks.

Data Transformation:
1. Applied transformations on the raw data to align with the predefined data model using Python.

Data Loading:
1. Transformed data is loaded into Google BigQuery for analysis using Airflow.

Data Visualization:
1. SQL queries are performed on BigQuery for analytical insights.
2. The processed data is visualized using Looker Studio to create dashboards.

Architecture
Tools and Technologies
1. Google Cloud Storage
2. Apache Airflow
3. Google Cloud Composer
4. Google BigQuery
5. Looker Studio

Key Features  
1. Scalable and efficient ETL pipeline.
2. Seamless integration with cloud-native tools.
3. Automation using Apache Airflow DAGs.

   ![image](https://github.com/user-attachments/assets/f80e676f-9d7b-4980-9e9e-14c63aa6851b)

   ![image](https://github.com/user-attachments/assets/0a833111-1308-42df-916b-4c51f3bd3e1f)

