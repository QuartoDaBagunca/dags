import boto3
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.models import Variable

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
    "email_on_retry": False
}

@dag(
    dag_id="pipeline_num_one",
    default_args=default_args, 
    schedule_interval='0 */6 * * *',
    catchup=False, 
    tags=["TESTE", "AWS"], 
    description="First AWS Pipeline")

def execute_emr():
  
    @task
    def emr_create_cluster(sucess_before: bool):
        """[Create Cluster EMR]
        Args:
            sucess_before (bool): []
        """        
        if sucess_before:
            cluster_id = client.run_job_flow(
                Name='EMR-RUN-ONE',  
                ServiceRole='EMR_DefaultRole',
                JobFlowRole='EMR_EC2_DefaultRole',
                VisibleToAllUsers=True,
                LogUri='s3://igti-projeto-aplicado/logs-emr/',
                ReleaseLabel='emr-6.4.0',
                Instances={
                    'InstanceGroups': [
                        {
                            'Name': 'Master nodes',
                            'Market': 'SPOT',
                            'InstanceRole': 'MASTER',
                            'InstanceType': 'r4.xlarge',
                            'InstanceCount': 1,
                        },
                        {
                            'Name': 'Worker nodes',
                            'Market': 'SPOT',
                            'InstanceRole': 'CORE',
                            'InstanceType': 'r4.xlarge',
                            'InstanceCount': 1,
                        }
                    ],
                    'Ec2KeyName': 'qb-emr-cluster',
                    "Placement": {
                        "AvailabilityZone": "us-east-1c"
                    },
                    'KeepJobFlowAliveWhenNoSteps': True,
                    'TerminationProtected': False
                },

                Tags=[
                        {
                            'Key': 'test-emr-cluster'
                        }
                    ],

                Applications=[{'Name': 'Spark'}],

                Configurations=[{
                    "Classification": "spark-env",
                    "Properties": {},
                    "Configurations": [{
                        "Classification": "export",
                        "Properties": {
                            "PYSPARK_PYTHON": "/usr/bin/python3",
                            "PYSPARK_DRIVER_PYTHON": "/usr/bin/python3"
                        }
                    }]
                },
                    {
                        "Classification": "spark-hive-site",
                        "Properties": {
                            "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
                        }
                    },
                    {
                        "Classification": "spark-defaults",
                        "Properties": {
                            "spark.submit.deployMode": "cluster",
                            "spark.speculation": "false",
                            "spark.sql.adaptive.enabled": "true",
                            "spark.serializer": "org.apache.spark.serializer.KryoSerializer"
                        }
                    },
                    {
                        "Classification": "spark",
                        "Properties": {
                            "maximizeResourceAllocation": "true"
                        }
                    }
                ],

                # BootstrapActions=[{   
                #             'Name': 'Bootstrap Structury Model',
                #             'ScriptBootstrapAction': {
                #             'Path': 's3://igti-projeto-aplicado/'}
                # }],
            )
            return cluster_id["JobFlowId"]


    @task
    def wait_emr_cluster_running(cid: str):
        waiter = client.get_waiter('cluster_running')
        waiter.wait(
            ClusterId=cid,
            WaiterConfig={
                'Delay': 30,
                'MaxAttempts': 120
            }
        )
        
        return True

    @task
    def emr_process_one(cid: str, success_before: bool):
        if success_before:
            job_output = client.add_job_flow_steps(
                JobFlowId=cid,
                Steps=[{
                    'Name': 'Step One',
                    'ActionOnFailure': 'TERMINATE_CLUSTER',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['spark-submit',
                                '--master', 'yarn',
                                '--deploy-mode', 'cluster',
                                '--conf',  'spark.kryoserializer.buffer.max=128m',
                                's3://igti-projeto-aplicado/scripts/proc_script_version.py'
                                ]
                    }
                }]
            )
            return job_output['StepIds'][0] 

    @task
    def wait_emr_process_one_step(cid: str, stepId: str):
        waiter = client.get_waiter('step_complete')
        steps = client.list_steps(
            ClusterId=cid
        )
        stepId = steps['Steps'][0]['Id']

        waiter.wait(
            ClusterId=cid,
            StepId=stepId,
            WaiterConfig={
                'Delay': 30,
                'MaxAttempts': 120
            }
        )
        return True

    @task
    def terminate_emr_cluster(success_before: str, cid: str):
        if success_before:
            client.terminate_job_flows(
                JobFlowIds=[cid]
            )
        return True

    # Encadeando a pipeline

    cluid = emr_create_cluster(True)
    clurun = wait_emr_cluster_running(cluid)
    emr_csv = emr_process_one(cluid,clurun)
    res_emr_csv = wait_emr_process_one_step(cluid, emr_csv)
    terminate_emr_cluster(res_emr_csv, cluid)

execucao = execute_emr()