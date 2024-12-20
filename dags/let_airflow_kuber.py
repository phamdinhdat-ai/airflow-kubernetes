import os 
from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesHook, KubernetesPodOperator, KubernetesPodOperatorCallback
from airflow.configuration import conf
from kubernetes.client import models as k8s 
from kubernetes_asyncio.client import models as k8s_async


DOCKER_IMAGE_1 = "datphamdinh/mongo-extract:v3"
DOCKER_IMAGE_2 = "datphamdinh/upload-ml-ch-db:1.0"
DOCKER_IMAGE_3 = "datphamdinh/load-aws:1.0"

@dag(
    dag_id='pipeline_LET',
    schedule="@daily",
    start_date=datetime.now(),
    end_date=datetime(2024, 12, 22),
    catchup=False, 
    tags=['MultiKubernetesPodOperator', "LET Flow"]
)
def let_flow():

    load_mongo_data = KubernetesPodOperator(
        task_id = "load_data_mongo",
        name='load_mongo_db',
        image = DOCKER_IMAGE_1,
        cmds=["python", "extract_mongo_clickhouse.py"],
        namespace= conf.get('kubernetes', 'NAMESPACE'),
        in_cluster=False, 
        hostnetwork=True, 
        hostname='localhost',
        get_logs=True, 
        log_events_on_failure=True, 
        log_pod_spec_on_failure=True, 
        is_delete_operator_pod=True, 
        do_xcom_push=True, 
        config_file='./include/.kube/config',
        ports=[k8s.V1ContainerPort(container_port=8080)],
        image_pull_secrets=[k8s.V1LocalObjectReference('my-registry-secret')],
        env_vars={
            'CLICKHOUSE_HOST': 'localhost',
            'CLICKHOUSE_USER': 'datpd1',
            'CLICKHOUSE_PASSWORD': 'datpd1',
            'DATABASE': 'houses',
            'TABLE': 'data_house_v2',
            'MONGO_DB': 'admin',
            'MONGO_COLLECTIONS': 'data_houses',
            'MONGO_HOST': '127.0.0.1',
            'MONGO_USER': 'user',
            'MONGO_PASSWORD': 'password',
            
        }

    )

    load_clickhouse = KubernetesPodOperator(
        task_id = "load_ml_clickhouse",
        name='upload_clickhouse',
        image = DOCKER_IMAGE_2,
        cmds=["python", "load_clickhousedb.py"],
        namespace=conf.get('kubernetes', 'NAMESPACE'),
        in_cluster=False, 
        hostnetwork=True, 
        hostname='localhost',
        get_logs=True, 
        log_events_on_failure=True, 
        # log_pod_spec_on_failure=True, 
        is_delete_operator_pod=True, 
        do_xcom_push=True, 
        config_file='./include/.kube/config',
        ports=[k8s.V1ContainerPort(container_port=8080)],
        image_pull_secrets=[k8s.V1LocalObjectReference('my-registry-secret')],
        # image_pull_policy='Always',
        env_vars={
            'CLICKHOUSE_HOST': 'localhost',
            'CLICKHOUSE_USER': 'datpd1',
            'CLICKHOUSE_PASSWORD': 'datpd1',
            'DATABASE': 'houses',
            'TABLE': 'data_house_v2',
            'TABLE_RESULT':'data_house_prediction_v3'
        }

    )
    ch_to_aws = KubernetesPodOperator(
        task_id = "clickhouse_to_aws",
        name='load_aws',
        image = DOCKER_IMAGE_3,
        cmds=["python", "load_aws.py"],
        namespace=conf.get('kubernetes', 'NAMESPACE'),
        in_cluster=False, 
        hostnetwork=True, 
        hostname='localhost',
        get_logs=True, 
        log_events_on_failure=True, 
        # log_pod_spec_on_failure=True, 
        is_delete_operator_pod=True, 
        do_xcom_push=True, 
        config_file='./include/.kube/config',
        ports=[k8s.V1ContainerPort(container_port=8080)],
        image_pull_secrets=[k8s.V1LocalObjectReference('my-registry-secret')],
        # image_pull_policy='Always',
        env_vars={
            'CLICKHOUSE_HOST': 'localhost',
            'CLICKHOUSE_USER': 'datpd1',
            'CLICKHOUSE_PASSWORD': 'datpd1',
            'DATABASE': 'houses',
            'TABLE': 'data_house_v2',
            'TABLE_RESULT' : 'data_house_prediction_v3',
            'BUCKET_NAME' : '...',
            'AWS_ACCESS_KEY' : '...',
            'AWS_SECRET' : '...',

        }

    )
    load_mongo_data >> load_clickhouse >> ch_to_aws
let_flow()