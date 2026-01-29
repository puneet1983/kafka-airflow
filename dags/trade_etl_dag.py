from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
# from scripts import kafka_consumer

import sys
sys.path.append("/opt/airflow/scripts")
# import kafka_test_consumer
import kafka_consumer_batch
import process_batch_file
import load_delta
from airflow.utils.email import send_email



default_args = {
    "owner": "puneet",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": True,  # optional
    "email_on_retry": False,  # optional
    "email_on_success": True,
    "email": ["puneet.jaiswal@gmail.com"]  
}

def send_task_email(subject, content,to="puneet.jaiswal@gmail.com"):
    """Reusable function to send email and log the attempt."""
    print(f"Attempting to send email to: {to}")
    try:
        send_email(to=to, subject=subject, html_content=content)
        print(f"Email sent to: {to}")
    except Exception as e:
        print(f"Failed to send email to {to}: {e}")

def log_success_email(context):
    task_id = context['task_instance'].task_id
    print(f"Attempting to send SUCCESS email for task: {task_id}")
    subject=f"Task {task_id} Succeeded",
    content=f"Task {task_id} completed successfully."
    send_task_email( subject, content)
    

def log_failure_email(context):
    task_id = context['task_instance'].task_id
    print(f"Attempting to send FAILURE email for task: {task_id}")
    subject=f"Task {task_id} Failed",
    html_content=f"Task {task_id} failed."
    send_task_email( subject, html_content)



with DAG(
    dag_id="trade_processing_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 26),
    schedule_interval="@daily",  # Or use "@daily"
    catchup=False
) as dag:
    
    
    wait_for_topic = PythonOperator(
        task_id="wait_for_topic",
        python_callable=kafka_consumer_batch.wait_for_topic,
        op_args=['trade-events', 'kafka:9092'],
        retries=3,
        retry_delay=timedelta(seconds=10)
    )

    # run_trade_consumer = PythonOperator(
    #     task_id="run_trade_consumer",
    #     python_callable=kafka_consumer.run_trade_consumer,
    #     retries=3,
    #     retry_delay=timedelta(seconds=10),
    #     on_success_callback=log_success_email,
    #     on_failure_callback=log_failure_email
    # )

    run_kafka_consumer = PythonOperator(
        task_id="run_kafka_consumer_batch",
        python_callable=kafka_consumer_batch.consume_kafka_to_file,
        op_args=["trade-events","kafka:9092" ,10, "/opt/airflow/raw/data"],
        retries=3,
        retry_delay=timedelta(seconds=10),
        on_success_callback=log_success_email,
        on_failure_callback=log_failure_email
    )

    process_trade_file = PythonOperator(
    task_id="process_staging_file",
    python_callable=process_batch_file.process_staging_file,
    op_args=[
    "{{ ti.xcom_pull(task_ids='run_kafka_consumer_batch')[0] }}",
    "{{ ti.xcom_pull(task_ids='run_kafka_consumer_batch')[1] }}"],
    retries=3,
    retry_delay=timedelta(seconds=10),
    on_success_callback=log_success_email,
    on_failure_callback=log_failure_email
)

    merge_staging_to_final = PythonOperator(
        task_id="insert_to_final_table",
        python_callable=load_delta.merge_staging_to_final,
        op_args=["{{ ti.xcom_pull(task_ids='run_kafka_consumer_batch')[1] }}"],
        retries=3,
        retry_delay=timedelta(seconds=10),
        on_success_callback=log_success_email,
        on_failure_callback=log_failure_email
    )

    wait_for_topic >> run_kafka_consumer>> process_trade_file >> merge_staging_to_final 
