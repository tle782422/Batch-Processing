import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Create DAG
dag = DAG(
    dag_id = "sparking_flow",
    default_args = {
        "owner": "Toan Le",
        "start_date": airflow.utils.dates.days_ago(1)
    },
    # schedule_interval = "@daily"
    # schedule_interval = None,
    # catchup = False  
)

# Start DAG
start = PythonOperator(
    task_id="start",
    python_callable = lambda: print("Jobs started"),
    dag=dag
)


# Create Schema HQL (OK)
hive_schema_job = SparkSubmitOperator(
    task_id="hive_schema_job",
    conn_id="spark-connect",
    application="jobs/Spark-Job-1.0-SNAPSHOT.jar",
    java_class="org.example.config.HiveSchema",
    dag=dag
)

# Ingestion Data
data_ingestion_job = SparkSubmitOperator(
    task_id="data_ingestion",
    conn_id="spark-connect",
    application="jobs/Spark-Job-1.0-SNAPSHOT.jar",
    java_class="org.example.ingestion.SparkPreprocessing",
    dag=dag
)

Process Data
spark_job = SparkSubmitOperator(
    task_id="spark_job",
    conn_id="spark-connect",
    application="jobs/Spark-Job-1.0-SNAPSHOT.jar",
    java_class="org.example.App",
    dag=dag
)

# End DAG
end = PythonOperator(
    task_id="end",
    python_callable = lambda: print("Jobs completed successfully"),
    dag=dag
)

start >> [ hive_schema_job, data_ingestion_job ] >> spark_job >> end
