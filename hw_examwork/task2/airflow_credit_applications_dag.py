from __future__ import annotations

import datetime
import uuid

from airflow import DAG
from airflow.providers.yandex.operators.yandexcloud_dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator,
)
from airflow.utils.trigger_rule import TriggerRule


YC_DP_AZ = "ru-central1-a"
YC_DP_SUBNET_ID = "e9bh2l7j2lg80kndcqvu"
YC_DP_SERVICE_ACCOUNT_ID = "ajeb4hgvpjb96ubkg76p"
YC_DP_SSH_PUBLIC_KEY = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIHdCxgoreE62QBc6wX+Vv72sjdHkSg6kKnWTd1swAcX0 r.hamidullin@oksk.ru"
YC_DP_METASTORE_URI = "10.128.0.20"
YC_BUCKET = "exametl"

INPUT_URI = f"s3a://{YC_BUCKET}/input/credit_applications_2026_05.csv"
OUTPUT_URI = f"s3a://{YC_BUCKET}/output"
PYSPARK_URI = f"s3a://{YC_BUCKET}/scripts/pyspark_credit_applications.py"


with DAG(
    dag_id="rus_credit_applications_dataproc",
    description="Create temporary Data Processing cluster, run PySpark ETL, delete cluster",
    schedule_interval=None,
    start_date=datetime.datetime(2026, 5, 1),
    catchup=False,
    max_active_runs=1,
    tags=["exam", "data-processing", "pyspark"],
) as dag:
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        cluster_name=f"rus-credit-apps-{uuid.uuid4()}",
        cluster_description="Temporary cluster for credit applications PySpark ETL",
        ssh_public_keys=YC_DP_SSH_PUBLIC_KEY,
        service_account_id=YC_DP_SERVICE_ACCOUNT_ID,
        subnet_id=YC_DP_SUBNET_ID,
        s3_bucket=YC_BUCKET,
        zone=YC_DP_AZ,
        cluster_image_version="2.1",
        masternode_resource_preset='s2.micro',   
        masternode_disk_type='network-ssd',
        masternode_disk_size=20,                 
        computenode_resource_preset='s2.micro',
        computenode_disk_type='network-ssd',
        computenode_disk_size=20,
        computenode_count=1,                   
        computenode_max_hosts_count=2,  
        services=['YARN', 'SPARK'],
        datanode_count=0,
        properties={
            "spark:spark.hive.metastore.uris": f"thrift://{YC_DP_METASTORE_URI}:9083",
        },
    )

    run_pyspark = DataprocCreatePysparkJobOperator(
        task_id="run_credit_applications_pyspark",
        cluster_id=create_cluster.output,
        main_python_file_uri=PYSPARK_URI,
        args=[
            "--input",
            INPUT_URI,
            "--output",
            OUTPUT_URI,
        ],
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        cluster_id=create_cluster.output,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    create_cluster >> run_pyspark >> delete_cluster
