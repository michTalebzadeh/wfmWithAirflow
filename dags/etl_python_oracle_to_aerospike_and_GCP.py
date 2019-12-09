from airflow import DAG
from airflow.operators import BashOperator
from datetime import datetime, timedelta
import os.path
from os import path
from datetime import datetime
import time, os, fnmatch, shutil
import pendulum

local_tz = pendulum.timezone("Europe/London")

# Following are defaults which can be overridden later on
default_args = {
    'owner': 'hduser',
    'depends_on_past': False,
    'start_date': datetime(2019, 11, 30),
    'email': ['mich.talebzadeh@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
     dag_id = 'etl_python_oracle_to_aerospike_and_GCP' ,
     default_args=default_args ,
     description='Taking data out of Oracle table into Aerospike set on prem and cloud plus bigQuery table' ,
     schedule_interval='0 * * * *' ,   
     params={
        "username": "hduser",
        "password": "hduser",
        "at": "@",
        "host1": "rhes75",
        "host2": "dpcluster-m"
     }
)

# Extract data from Oracle table to a flat file
create_command1 = 'su {{ params.username }} -c "/home/hduser/dba/bin/python/etl_python_oracle_to_aerospike_and_GCP.ksh -O 1 > "/d4T/hduser/airflow/run_logs/t1_etl_python_oracle_to_aerospike_and_GCP_extract_data_from_oracle"_"`date +%Y%m%d_%H%M`"."log" 2>&1 "'
t1 = BashOperator(
    task_id='t1_Extract_data_from_Oracle_table_to_a_flat_file',
    bash_command=create_command1,
    dag=dag)


# Read flat file and save it to an aerospike set on prem
create_command2 = 'su {{ params.username }} -c "/home/hduser/dba/bin/python/etl_python_oracle_to_aerospike_and_GCP.ksh -O 2 > "/d4T/hduser/airflow/run_logs/t2_etl_python_oracle_to_aerospike_and_GCP_save_to_aerospike_set_on_prem"_"`date +%Y%m%d_%H%M`"."log" 2>&1 "'
t2 = BashOperator(
    task_id='t2_Read_flat_file_and_save_it_to_an_aerospike_set_on_prem',
    bash_command=create_command2,
    dag=dag)

# Read and confirm that data stored in aerospike set on prem
create_command3 = 'su {{ params.username }} -c "/home/hduser/dba/bin/python/etl_python_oracle_to_aerospike_and_GCP.ksh -O 3 > "/d4T/hduser/airflow/run_logs/t3_etl_python_oracle_to_aerospike_and_GCP_read_from_aerospike_set_on_prem"_"`date +%Y%m%d_%H%M`"."log" 2>&1 "'
t3 = BashOperator(
    task_id='t3_Read_and_confirm_that_data_stored_in_aerospike_set_on_prem',
    bash_command=create_command3,
    dag=dag)

# scp the csv file to the cloud host

create_command4 = 'su {{ params.username }} -c "/usr/bin/sshpass -p "{{ params.password }}" /usr/bin/scp -v -r {{ params.username }}{{ params.at }}{{ params.host1 }}:/backup/hduser/DUMMY.csv {{ params.username }}{{ params.at }}{{ params.host2 }}:/backup/hduser/DUMMY.csv > "/d4T/hduser/airflow/run_logs/t4_scp_DUMMY.csv_to_{{ params.host2 }}"_"`date +%Y%m%d_%H%M`".log 2>&1 "'
t4 = BashOperator(
    task_id='t4_scp_the_csv_file_to_the_cloud_host',
    bash_command=create_command4,
    dag=dag
)

# Read flat file and save it to an aerospike set on cloud dpcluster-m
# note you are revoking the python command in cloud host through ssh
create_command5 = 'su {{ params.username }} -c "/usr/bin/sshpass -p "{{ params.password }}" /usr/bin/ssh -v {{ params.username }}{{ params.at }}{{ params.host2 }} /usr/bin/python /home/hduser/dba/bin/python/etl_python_oracle_to_aerospike_and_GCP/src/etl_python_oracle_to_aerospike_and_GCP.py 2 > "/d4T/hduser/airflow/run_logs/t5_{{ params.host2 }}_etl_python_oracle_to_aerospike_and_GCP_save_to_aerospike_set_on_docker"_"`date +%Y%m%d_%H%M`".log 2>&1"'
t5 = BashOperator(
     task_id='t5_Read_flat_file_and_save_it_to_an_aerospike_set_on_cloud',
    bash_command=create_command5,
    dag=dag)

# Read and confirm that data stored in aerospike set on cloud dpcluster-m
# note you are revoking the python command in cloud host through ssh
create_command6 = 'su {{ params.username }} -c "/usr/bin/sshpass -p "{{ params.password }}" /usr/bin/ssh -v {{ params.username }}{{ params.at }}{{ params.host2 }} /usr/bin/python /home/hduser/dba/bin/python/etl_python_oracle_to_aerospike_and_GCP/src/etl_python_oracle_to_aerospike_and_GCP.py 3 > "/d4T/hduser/airflow/run_logs/t6_{{ params.host2 }}_etl_python_oracle_to_aerospike_and_GCP_read_from_aerospike_set_on_docker"_"`date +%Y%m%d_%H%M`".log 2>&1"'
t6 = BashOperator(
    task_id='t6_Read_and_confirm_that_data_stored_in_aerospike_set_on_cloud',
    bash_command=create_command6,
    dag=dag)

# copy the file to GCP bucket and delete it if already exists
create_command7 = 'su {{ params.username }} -c "/home/hduser/dba/bin/python/etl_python_oracle_to_aerospike_and_GCP.ksh -O 4 > "/d4T/hduser/airflow/run_logs/t7_etl_python_oracle_to_aerospike_and_GCP_upload_file_to_GCP_bucket"_"`date +%Y%m%d_%H%M`"."log" 2>&1 "'
t7 = BashOperator(
    task_id='t7_copy_the_file_to_GCP_bucket_and_delete_it_if_already_exists',
    bash_command=create_command7,
    dag=dag)

# drop and create BigQuery table if exists
create_command8 = 'su {{ params.username }} -c "/home/hduser/dba/bin/python/etl_python_oracle_to_aerospike_and_GCP.ksh -O 5 > "/d4T/hduser/airflow/run_logs/t8_etl_python_oracle_to_aerospike_and_GCP_drop_and_create_BigQuery_table"_"`date +%Y%m%d_%H%M`"."log" 2>&1 "'
t8 = BashOperator(
    task_id='t8_drop_and_create_BigQuery_table_if_exists',
    bash_command=create_command8,
    dag=dag)

# Load data into BigQuery table from csv file
create_command9 = 'su {{ params.username }} -c "/home/hduser/dba/bin/python/etl_python_oracle_to_aerospike_and_GCP.ksh -O 6 > "/d4T/hduser/airflow/run_logs/t9_etl_python_oracle_to_aerospike_and_GCP_load_data_into_BigQuery_table_from_CSV_file"_"`date +%Y%m%d_%H%M`"."log" 2>&1 "'
t9 = BashOperator(
    task_id='t9_Load_data_into_BigQuery_table_from_csv_file',
    bash_command=create_command9,
    dag=dag)

# Read data from BigQuery table
create_command10 = 'su {{ params.username }} -c "/home/hduser/dba/bin/python/etl_python_oracle_to_aerospike_and_GCP.ksh -O 7 > "/d4T/hduser/airflow/run_logs/t10_etl_python_oracle_to_aerospike_and_GCP_Read_data_from_BigQuery_table"_"`date +%Y%m%d_%H%M`"."log" 2>&1 "'
t10 = BashOperator(
    task_id='t10_Read_data_from_BigQuery_table',
    bash_command=create_command10,
    dag=dag)


t2.set_upstream(t1)
t3.set_upstream(t2)
t4.set_upstream(t1)
t5.set_upstream(t4)
t6.set_upstream(t5)
t7.set_upstream(t1)
t8.set_upstream(t7)
t9.set_upstream(t8)
t10.set_upstream(t9)
