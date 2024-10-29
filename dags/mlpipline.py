from airflow import DAG
from airflow.oparators.python import PythonOperator
from datetime import datetime

## define task
def preprocess():
    print('Preprocess data....')

def train_model():
    print('Traing model ...')

def eval_model():
    print('eval model ...')