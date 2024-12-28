from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.cloud import bigquery
import pandas as pd
from datetime import datetime, timedelta

# Define DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'uber_data_etl',
    default_args=default_args,
    description='ETL pipeline for Uber data',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    def download_data_from_gcs(**kwargs):
        """Download data from GCS to a temporary location."""
        bucket_name = "uber_data_project_ms"
        file_name = "uber_data.csv"
        gcs_hook = GCSHook()
        client = gcs_hook.get_conn()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        local_file_path = f"/tmp/{file_name}"
        blob.download_to_filename(local_file_path)
        kwargs['ti'].xcom_push(key='local_file_path', value=local_file_path)

    def transform_data(**kwargs):
        """Perform data transformations using pandas."""
        local_file_path = kwargs['ti'].xcom_pull(key='local_file_path', task_ids='download_data')
        df = pd.read_csv(local_file_path)

        # Data transformations (as per your existing logic)
        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
        df = df.drop_duplicates().reset_index(drop=True)
        df['trip_id'] = df.index

        datetime_dim = df[['tpep_pickup_datetime', 'tpep_dropoff_datetime']].drop_duplicates().reset_index(drop=True)
        datetime_dim['pick_hour'] = datetime_dim['tpep_pickup_datetime'].dt.hour
        datetime_dim['pick_day'] = datetime_dim['tpep_pickup_datetime'].dt.day
        datetime_dim['pick_month'] = datetime_dim['tpep_pickup_datetime'].dt.month
        datetime_dim['pick_year'] = datetime_dim['tpep_pickup_datetime'].dt.year
        datetime_dim['pick_weekday'] = datetime_dim['tpep_pickup_datetime'].dt.weekday
        datetime_dim['drop_hour'] = datetime_dim['tpep_dropoff_datetime'].dt.hour
        datetime_dim['drop_day'] = datetime_dim['tpep_dropoff_datetime'].dt.day
        datetime_dim['drop_month'] = datetime_dim['tpep_dropoff_datetime'].dt.month
        datetime_dim['drop_year'] = datetime_dim['tpep_dropoff_datetime'].dt.year
        datetime_dim['drop_weekday'] = datetime_dim['tpep_dropoff_datetime'].dt.weekday
        datetime_dim['datetime_id'] = datetime_dim.index
        datetime_dim = datetime_dim[['datetime_id', 'tpep_pickup_datetime', 'pick_hour', 'pick_day', 'pick_month', 'pick_year', 'pick_weekday',
                                    'tpep_dropoff_datetime', 'drop_hour', 'drop_day', 'drop_month', 'drop_year', 'drop_weekday']]

        passenger_count_dim = df[['passenger_count']].drop_duplicates().reset_index(drop=True)
        passenger_count_dim['passenger_count_id'] = passenger_count_dim.index
        passenger_count_dim = passenger_count_dim[['passenger_count_id', 'passenger_count']]

        trip_distance_dim = df[['trip_distance']].drop_duplicates().reset_index(drop=True)
        trip_distance_dim['trip_distance_id'] = trip_distance_dim.index
        trip_distance_dim = trip_distance_dim[['trip_distance_id', 'trip_distance']]

        rate_code_type = {
            1: "Standard rate",
            2: "JFK",
            3: "Newark",
            4: "Nassau or Westchester",
            5: "Negotiated fare",
            6: "Group ride"
        }
        rate_code_dim = df[['RatecodeID']].drop_duplicates().reset_index(drop=True)
        rate_code_dim['rate_code_id'] = rate_code_dim.index
        rate_code_dim['rate_code_name'] = rate_code_dim['RatecodeID'].map(rate_code_type)
        rate_code_dim = rate_code_dim[['rate_code_id', 'RatecodeID', 'rate_code_name']]

        pickup_location_dim = df[['pickup_longitude', 'pickup_latitude']].drop_duplicates().reset_index(drop=True)
        pickup_location_dim['pickup_location_id'] = pickup_location_dim.index
        pickup_location_dim = pickup_location_dim[['pickup_location_id', 'pickup_latitude', 'pickup_longitude']]

        dropoff_location_dim = df[['dropoff_longitude', 'dropoff_latitude']].drop_duplicates().reset_index(drop=True)
        dropoff_location_dim['dropoff_location_id'] = dropoff_location_dim.index
        dropoff_location_dim = dropoff_location_dim[['dropoff_location_id', 'dropoff_latitude', 'dropoff_longitude']]

        payment_type_name = {
            1: "Credit card",
            2: "Cash",
            3: "No charge",
            4: "Dispute",
            5: "Unknown",
            6: "Voided trip"
        }
        payment_type_dim = df[['payment_type']].drop_duplicates().reset_index(drop=True)
        payment_type_dim['payment_type_id'] = payment_type_dim.index
        payment_type_dim['payment_type_name'] = payment_type_dim['payment_type'].map(payment_type_name)
        payment_type_dim = payment_type_dim[['payment_type_id', 'payment_type', 'payment_type_name']]

        fact_table = df.merge(passenger_count_dim, left_on='trip_id', right_on='passenger_count_id') \
                    .merge(trip_distance_dim, left_on='trip_id', right_on='trip_distance_id') \
                    .merge(rate_code_dim, left_on='trip_id', right_on='rate_code_id') \
                    .merge(pickup_location_dim, left_on='trip_id', right_on='pickup_location_id') \
                    .merge(dropoff_location_dim, left_on='trip_id', right_on='dropoff_location_id') \
                    .merge(datetime_dim, left_on='trip_id', right_on='datetime_id') \
                    .merge(payment_type_dim, left_on='trip_id', right_on='payment_type_id') \
                    [['trip_id', 'VendorID', 'datetime_id', 'passenger_count_id',
                    'trip_distance_id', 'rate_code_id', 'store_and_fwd_flag', 'pickup_location_id', 'dropoff_location_id',
                    'payment_type_id', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
                    'improvement_surcharge', 'total_amount']]

        tables = {
            'datetime_dim': datetime_dim,
            'passenger_count_dim': passenger_count_dim,
            'trip_distance_dim': trip_distance_dim,
            'rate_code_dim': rate_code_dim,
            'pickup_location_dim': pickup_location_dim,
            'dropoff_location_dim': dropoff_location_dim,
            'payment_type_dim': payment_type_dim,
            'fact_table': fact_table
        }

        transformed_file_paths = {}
        for table_name, table_df in tables.items():
            path = f"/tmp/{table_name}.csv"
            table_df.to_csv(path, index=False)
            transformed_file_paths[table_name] = path

        kwargs['ti'].xcom_push(key='transformed_file_paths', value=transformed_file_paths)

    def load_to_bigquery(**kwargs):
        """Load transformed data into BigQuery."""
        transformed_file_paths = kwargs['ti'].xcom_pull(key='transformed_file_paths', task_ids='transform_data')
        client = bigquery.Client()

        for table_name, file_path in transformed_file_paths.items():
            table_id = f"uber-445501.uber.{table_name}"  # Replace with your project and dataset
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
                autodetect=True,
            )
            with open(file_path, 'rb') as file:
                load_job = client.load_table_from_file(file, table_id, job_config=job_config)

            load_job.result()  # Wait for the job to complete
            print(f"Loaded {load_job.output_rows} rows into {table_id}.")

    # Define Tasks
    download_data = PythonOperator(
        task_id='download_data',
        python_callable=download_data_from_gcs,
        provide_context=True,
    )

    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

    load_data = PythonOperator(
        task_id='load_to_bigquery',
        python_callable=load_to_bigquery,
        provide_context=True,
    )

    # Set Task Dependencies
    download_data >> transform_data >> load_data
