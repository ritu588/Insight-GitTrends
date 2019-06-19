'''
Data Download DAG
This airflow data downloadpipeline is for :
1) downloading historic data from github archive to S3
'''
# import airflow packages
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
# other packages
from datetime import datetime
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 01, 31),
    'end_date':datetime(2017, 02, 02),
    'email_on_failure': False,
    'email_on_retry': False,
    'schedule_interval': '@hourly',
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

#initial dag object
dag = DAG(
  dag_id='data_download_dag',
  description='data download from github archive -> extract and save to s3',
  default_args=default_args)

#step1 data download
data_download = BashOperator(
  task_id='data_download',
  bash_command='python /home/ubuntu/botodownload.py',
  dag = dag)
