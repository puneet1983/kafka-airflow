from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

import sys
sys.path.append("/opt/airflow/scripts")
import trade_generator

default_args = {
    "owner": "puneet",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,  # optional
    "email_on_retry": False,  # optional
    "email": ["puneet@gmail.com"]  # configure SMTP if needed
}



with DAG(
    dag_id="trade_simulation",
    default_args=default_args,
    start_date=datetime(2026, 1, 26),
    schedule_interval="@daily",  # Or use "@daily"
    catchup=False
) as dag:
    
    # run_trade_generator = BashOperator(
    #     task_id="run_trade_generator",
    #     bash_command="python /opt/airflow/scripts/trade_generator.py"
    # )

     # your script module


    ensure_topic = PythonOperator(
        task_id="ensure_kafka_topic",
        python_callable=trade_generator.ensure_topic,
        op_args=['trade-events', 'kafka:9092'],
        retries=3,
        retry_delay=timedelta(seconds=10)
    )

    generate_and_publish = PythonOperator(
        task_id="generate_and_publish_trades",
        python_callable=trade_generator.generate_trade,
        op_args=[10,'trade-events'],   # <-- number of trades
        retries=3,
        retry_delay=timedelta(seconds=10)
    )

    ensure_topic >> generate_and_publish
