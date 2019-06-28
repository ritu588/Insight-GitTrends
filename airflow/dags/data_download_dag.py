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
    'schedule_interval': timedelta(minutes=60),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

#initial dag object
dag = DAG(
  dag_id='data_manipulation_dagv2',
  description='data download from github archive -> extract and save to s3',
  default_args=default_args)

#step1 data download
data_manipulationv2 = BashOperator(
  task_id='data_manipulationv2',
  bash_command='python /home/ubuntu/hourly_download.py',
  dag = dag)

#step2 data processing
spark_path = '/usr/local/spark/bin/spark-submit '
new_path = '/home/ubuntu/hourly_run.sh'
spark_command = spark_path + '--packages org.postgresql:postgresql:9.4.1208 /home/ubuntu/HourlyUpload.py'
spark_masterv2 = SSHOperator(
  ssh_conn_id='ssh_default',
  task_id='spark_masterv2',
  command=spark_command,
  dag = dag)

data_manipulationv2 >> spark_masterv2
