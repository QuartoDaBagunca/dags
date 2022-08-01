import boto3
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow import DAG

from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

aws_access_key_id = Variable.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key = Variable.get("AWS_SECRET_ACCESS_KEY")

client = boto3.client("emr", region_name="us-east-1",
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key)

default_args = {
    'owner': 'user',
    "depends_on_past": False,
    "start_date": days_ago(2),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id="pipeline_num_two",
    default_args=default_args, 
    schedule_interval='0 */6 * * *',
    catchup=False, 
    tags=["TESTE", "Kubernetes"], 
    description="First Kubernetes Pipeline"
) as dag:

    crawler_jox_raw = KubernetesPodOperator(
        task_id = "k8soperator",
        namespace = 'airflow_dev',
        image = 'canariodocker/pysparkproject',
        # arguments=['-Mbignum=bpi', '-wle', 'print bpi(2000)'],
        arguments = [
          '/opt/spark/bin/spark-submit'
          ,'--master'
          ,'local[2]'
          ,'main.py'
          ,'--nfunc'
          ,'2'
          ,'--origin'
          ,'s3a://igti-projeto-aplicado/processing-zone/prata'
          ,'--target'
          ,'s3a://igti-projeto-aplicado/processing-zone/ouro/dim/'
          ,'--iswrite'
          ,'1'
        ],
        name = 'execute-k8soperator',
        env_vars = {'AWS_ACCESS_KEY_ID': aws_access_key_id,
                    'AWS_SECRET_ACCESS_KEY': aws_secret_access_key},
        is_delete_operator_pod = True,
        in_cluster = True,
        get_logs = True
    )
    
    crawler_jox_raw