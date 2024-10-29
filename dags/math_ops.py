from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

## Define function for each task

def star_num(**context):
    context['ti'].xcom_push(key='current_value', value=10)
    print('Starting number 10')

def add_five(**context):
    current_value = context['ti'].xcom_pull(key='current_value', task_ids='start_task')
    new_value = current_value + 5
    context['ti'].xcom_push(key='current_value', value=new_value)
    print(f'add 5: {current_value} + 5 = {new_value}')

def multiply_two(**context):
    current_value = context['ti'].xcom_pull(key='current_value', task_ids='add_5_task')
    new_value = current_value * 2
    context['ti'].xcom_push(key='current_value', value=new_value)  # Push the new value
    print(f'multiply 2: {current_value} * 2 = {new_value}')

def sub_three(**context):
    current_value = context['ti'].xcom_pull(key='current_value', task_ids='multiply_2_task')
    new_value = current_value - 3
    context['ti'].xcom_push(key='current_value', value=new_value)  # Push the new value
    print(f'subtract 3: {current_value} - 3 = {new_value}')

def final(**context):
    current_value = context['ti'].xcom_pull(key='current_value', task_ids='sub_3_task')
    new_value = current_value ** 2
    print(f'squared result: {current_value} ^ 2 = {new_value}')

with DAG(
    dag_id='math_sequence_dag',  # Fixed typo here
    start_date=datetime(2024, 10, 29),
    schedule_interval='@once',
    catchup=False  # It's a good practice to set catchup for one-time DAGs
) as dag:
    
    # Define tasks
    start_task = PythonOperator(
        task_id='start_task',
        python_callable=star_num,
        provide_context=True
    )
    add_five_task = PythonOperator(
        task_id='add_5_task',
        python_callable=add_five,
        provide_context=True
    )
    multiply_2_task = PythonOperator(
        task_id='multiply_2_task',
        python_callable=multiply_two,
        provide_context=True
    )
    sub_3_task = PythonOperator(
        task_id='sub_3_task',
        python_callable=sub_three,
        provide_context=True
    )
    square_task = PythonOperator(  # Renamed for clarity
        task_id='square_num_task',
        python_callable=final,
        provide_context=True
    )

    # Define dependencies
    start_task >> add_five_task >> multiply_2_task >> sub_3_task >> square_task
