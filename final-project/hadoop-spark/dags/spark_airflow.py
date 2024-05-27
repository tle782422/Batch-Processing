import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.utils.dates import days_ago
from datetime import timedelta

# Default arguments
default_args = {
    "owner": "Toan Le",
    "start_date": days_ago(1),
    "retries": 1,
}

# Create DAG
dag = DAG(
    dag_id="sparking_flow",
    default_args=default_args,
    schedule_interval="@daily",  # Set the schedule interval
    catchup=False  # Set catchup to False
)

# Start task
start = PythonOperator(
    task_id="start",
    python_callable=lambda: print("Jobs started"),
    dag=dag
)

# 10-second delay task after start
delay_after_start = TimeDeltaSensor(
    task_id='delay_after_start',
    delta=timedelta(seconds=10),
    dag=dag
)

# Create Schema HQL task
hive_schema_job = SparkSubmitOperator(
    task_id="hive_schema_job",
    conn_id="spark-connect",
    application="jobs/Spark-Job-1.0-SNAPSHOT.jar",
    java_class="org.example.config.HiveSchemaCreate",
    dag=dag
)

# Data Ingestion task
data_ingestion_job = SparkSubmitOperator(
    task_id="data_ingestion",
    conn_id="spark-connect",
    application="jobs/Spark-Job-1.0-SNAPSHOT.jar",
    java_class="org.example.ingestion.SparkPreprocessing",
    dag=dag
)

# 10-second delay task after data_ingestion_job
delay_after_ingestion = TimeDeltaSensor(
    task_id='delay_after_ingestion',
    delta=timedelta(seconds=10),
    dag=dag
)

# Process Data task
spark_job = SparkSubmitOperator(
    task_id="spark_job",
    conn_id="spark-connect",
    application="jobs/Spark-Job-1.0-SNAPSHOT.jar",
    java_class="org.example.App",
    dag=dag
)

# End task
end = PythonOperator(
    task_id="end",
    python_callable=lambda: print("Jobs completed successfully"),
    dag=dag
)

# Set task dependencies
start >> delay_after_start >> [hive_schema_job, data_ingestion_job] >> delay_after_ingestion >> spark_job >> end
