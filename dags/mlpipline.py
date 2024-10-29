from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

## define task
def preprocess():
    print('Preprocess data....')

def train_model():
    print('Traing model ...')

def eval_model():
    print('eval model ...')

# define DAG
with DAG(
    'mlpipline',
    start_date=datetime(2024,1,29),
    schedule_interval='@weekly'
) as dag:
    preprocess=PythonOperator(task_id='preprocess_task',python_callable=preprocess)
    train=PythonOperator(task_id='train_task',python_callable=train_model)
    eval=PythonOperator(task_id='eval_task',python_callable=eval_model)

    # set dependences
    preprocess >> train >> eval

