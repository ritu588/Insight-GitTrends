'''
Data Download and processing DAG
This airflow data downloadpipeline is for :
1) downloading data from github archive to S3
2) Load the downloaded data to spark, process it and upload to database
'''
# import airflow packages
from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
# other packages
from datetime import datetime
from datetime import timedelta
#
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 06, 27),
    'email_on_failure': False,
    'email_on_retry': False,
    'schedule_interval': '10 */1 * * *',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

#initial dag object
dag = DAG(
  schedule_interval='10 */1 * * *',
  dag_id='data_download_dag',
  description='data download from github archive -> extract and save to s3',
  default_args=default_args)

#step1 data download
data_download = BashOperator(
  task_id='data_download',
  bash_command='python /home/ubuntu/hourly_download.py',
  dag = dag)

#step2 data processing
spark_path = '/usr/local/spark/bin/spark_submit '
spark_command = spark_path + '--packages org.postgresql:postgresql:9.4.1208 /home/ubuntu/HourlyUpload.py'
spark_master = SSHOperator(
  ssh_conn_id='ssh_default'
  task_id='spark_master',
  bash_command=spark_command,
  dag = dag)

 data_download >> spark_master
