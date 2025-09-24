from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
# Define the DAG
default_args = {
   'start_date': datetime(2023, 1, 1),
   'retries': 1,
}
with DAG(
   dag_id='hello_world_dag',
   default_args=default_args,
   schedule_interval='@daily', # Runs daily
   catchup=False,
) as dag:
   # Define tasks
   task_hello = BashOperator(
       task_id='say_hello',
       bash_command='echo "Hello, World!"'
   )
   task_goodbye = BashOperator(
       task_id='say_goodbye',
       bash_command='echo "Goodbye, World!"'
   )
   # Set task dependencies
   task_hello >> task_goodbye