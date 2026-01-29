# /opt/airflow/dags/trade_processing_pipeline.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.email import send_email
import sys

# Add scripts and config folder to path
sys.path.append("/opt/airflow/scripts")
sys.path.append("/opt/airflow/config")

import kafka_consumer_batch
import process_batch_file
import load_delta
import config  # <- our new config module

# ---------------- Default Args ----------------
default_args = {
    "owner": "puneet",
    "depends_on_past": False,
    "retries": config.DEFAULT_RETRIES,
    "retry_delay": timedelta(minutes=config.RETRY_DELAY_MINUTES),
    "email_on_failure": config.EMAIL_ON_FAILURE,
    "email_on_retry": config.EMAIL_ON_RETRY,
    "email_on_success": config.EMAIL_ON_SUCCESS,
    "email": config.EMAIL_LIST
}

# ---------------- Email Functions ----------------
def send_task_email(subject, content, to=config.EMAIL_LIST[0]):
    print(f"Attempting to send email to: {to}")
    try:
        send_email(to=to, subject=subject, html_content=content)
        print(f"Email sent to: {to}")
    except Exception as e:
        print(f"Failed to send email to {to}: {e}")

def log_success_email(context):
    task_id = context['task_instance'].task_id
    subject = f"Task {task_id} Succeeded"
    content = f"Task {task_id} completed successfully."
    send_task_email(subject, content)

def log_failure_email(context):
    task_id = context['task_instance'].task_id
    subject = f"Task {task_id} Failed"
    content = f"Task {task_id} failed."
    send_task_email(subject, content)

# ---------------- DAG Definition ----------------
with DAG(
    dag_id="trade_processing_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 26),
    schedule_interval="@daily",
    catchup=False
) as dag:

    wait_for_topic = PythonOperator(
        task_id="wait_for_topic",
        python_callable=kafka_consumer_batch.wait_for_topic,
        op_args=[config.KAFKA_TOPIC, config.KAFKA_BROKER],
        retries=config.DEFAULT_RETRIES,
        retry_delay=timedelta(seconds=config.RETRY_DELAY_SECONDS)
    )

    run_kafka_consumer = PythonOperator(
        task_id="run_kafka_consumer_batch",
        python_callable=kafka_consumer_batch.consume_kafka_to_file,
        op_args=[config.KAFKA_TOPIC, config.KAFKA_BROKER, config.KAFKA_BATCH_SIZE, config.RAW_DATA_PATH],
        retries=config.DEFAULT_RETRIES,
        retry_delay=timedelta(seconds=config.RETRY_DELAY_SECONDS),
        on_success_callback=log_success_email,
        on_failure_callback=log_failure_email
    )

    def process_file_callable(**context):
        file_path, batch_id = context['ti'].xcom_pull(task_ids='run_kafka_consumer_batch')
        return process_batch_file.process_staging_file(file_path, batch_id)

    process_trade_file = PythonOperator(
        task_id="process_staging_file",
        python_callable=process_file_callable,
        provide_context=True,
        retries=config.DEFAULT_RETRIES,
        retry_delay=timedelta(seconds=config.RETRY_DELAY_SECONDS),
        on_success_callback=log_success_email,
        on_failure_callback=log_failure_email
    )

    merge_staging_to_final = PythonOperator(
        task_id="insert_to_final_table",
        python_callable=load_delta.merge_staging_to_final,
        op_args=["{{ ti.xcom_pull(task_ids='run_kafka_consumer_batch')[1] }}"],
        retries=config.DEFAULT_RETRIES,
        retry_delay=timedelta(seconds=config.RETRY_DELAY_SECONDS),
        on_success_callback=log_success_email,
        on_failure_callback=log_failure_email
    )

    # ---------------- Task Dependencies ----------------
    wait_for_topic >> run_kafka_consumer >> process_trade_file >> merge_staging_to_final
