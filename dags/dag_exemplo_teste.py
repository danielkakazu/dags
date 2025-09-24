from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


# Função Python para teste
def print_hello():
    print("Hello from PythonOperator!")

# Argumentos padrão
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# Definição da DAG
with DAG(
    dag_id="dag_exemplo_teste",
    default_args=default_args,
    description="DAG simples de teste",
    schedule_interval="@daily",   # executa uma vez por dia
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["teste"],
) as dag:

    # Task usando PythonOperator
    task_python = PythonOperator(
        task_id="print_hello_task",
        python_callable=print_hello,
    )

    # Task usando BashOperator
    task_bash = BashOperator(
        task_id="bash_echo_task",
        bash_command="echo 'Hello from BashOperator!'",
    )

    # Dependência: task_python roda antes de task_bash
    task_python >> task_bash
