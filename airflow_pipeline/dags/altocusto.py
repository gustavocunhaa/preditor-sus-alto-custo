import sys
sys.path.append("airflow_pipeline")

from airflow.models import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

estados = []

with DAG(dag_id = "AltoCustDAG", 
         start_date=days_ago(0), 
         schedule_interval='0 11 10 * *') as dag:

    datasus_collector = DockerOperator(
        task_id="sih_collect",
        docker_url="tcp://docker-proxy:2375",
        api_version="auto",
        auto_remove=True,
        image="pysus-coletor",
        container_name="datasus_collect",
        command=[
            "--ano", str('{{ execution_date.year }}'),
            "--mes", int('{{ execution_date.month }}')
            ]
        )

    datasus_transform = SparkSubmitOperator(
        task_id="altocusto_transformation",
        application="/home/gustavo-cunha/Documentos/GitHub/preditor-sus-alto-custo/src/pipe/transform.py",
        application_args=[
            "--ano", str('{{ execution_date.year }}'),
            "--mes", int('{{ execution_date.month }}')
            ]
        )
    
    datasus_model = DockerOperator(
        task_id="alto_custo_model",
        docker_url="tcp://docker-proxy:2375",
        api_version="auto",
        auto_remove=True,
        image="altocusto-ml-classifier",
        container_name="datasus_model"
        )

datasus_collector >> datasus_transform >> datasus_model