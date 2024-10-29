from airflow import DAG
from airflow.decorators import task
from datetime import datetime

## define DAG

with DAG(
    dag_id='math_squence_dag_taskflow',
    start_date=datetime(2024,10,29),
    schedule='@once',
    catchup=False
) as dag:
    
    # task 1: start with intial num
    @task
    def start_num():
        inital_value=10
        print(f' starting num : {inital_value}')
        return inital_value
    
    # task 2
    def add_five(number):
        new_value = number + 5
        
        print(f'add 5: {number} + 5 = {new_value}')
        return new_value

    def multiply_two(number):
    
        new_value = number * 2
        # Push the new value
        print(f'multiply 2: {number} * 2 = {new_value}')
        return new_value

    def sub_three(number):

        new_value = number - 3
        # Push the new value
        print(f'subtract 3: {number} - 3 = {new_value}')
        return new_value
    def final(number):
        
        new_value = number ** 2
        print(f'squared result: {number} ^ 2 = {new_value}')
        return new_value
    
    ## set task dependencies
    start_value=start_num()
    add_value=add_five(start_value)
    sub_value=sub_three(add_value)
    multiply_value=multiply_two(sub_value)
    squre_value=final(multiply_value)