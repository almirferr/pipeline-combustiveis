from airflow.models import Variable
from os import getenv, path
from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator

# [START env_variables]
SPARK_NAMESPACE = getenv("SPARK_NAMESPACE", "processing")
# [END env_variables]

# [START variables]
DAGS_FOLDER_PATH = path.dirname(__file__)
# [END variables]

# [START instantiate_dag]
with DAG(
    dag_id='pipeline_teste',
    schedule_interval=None,
    start_date=datetime(2022, 10, 4),
    catchup=False,
    max_active_runs=1,
    tags=['teste', "kubernetes-pod-operator", 'spark-operator', 'k8s'],
) as dag:
# [END instantiate_dag]

    teste = KubernetesPodOperator(
          task_id="teste",
          name="pipeline-teste",
          is_delete_operator_pod=False,
          namespace=SPARK_NAMESPACE,
          startup_timeout_seconds=120,
          pod_template_file=f"{DAGS_FOLDER_PATH}/pipeline-teste.yaml",
          in_cluster=True,
          get_logs=True,
          env_vars = {
            "SOURCE_URLS" :  Variable.get("combustiveis_source_urls")
          }
      )

    # [START task_sequence]
    teste
    # [END task_sequence]
