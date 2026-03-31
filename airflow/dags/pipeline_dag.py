from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

default_args = {
    "owner": "sophia",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "realtime_anomaly_pipeline",
    default_args=default_args,
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    consumer = DockerOperator(
        task_id="run_consumer",
        image="consumer:latest",
        command="python consumer.py",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="airflow-net",
    )

    spark_batch = DockerOperator(
        task_id="run_spark_batch",
        image="sparkjob:latest",
        command="spark-submit /app/sparkjob.py",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="airflow-net",
    )

    ml = DockerOperator(
        task_id="run_ml_training",
        image="ml:latest",
        command="python train_model.py",
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="airflow-net",
    )

    consumer >> spark_batch >> ml
