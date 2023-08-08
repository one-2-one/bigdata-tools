import json
import logging
from datetime import timedelta
import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models.baseoperator import chain
from airflow.operators.python import get_current_context
from airflow.providers.amazon.aws.operators.eks import EksPodOperator
from airflow.utils.trigger_rule import TriggerRule
from kubernetes.client import models as k8s

log = logging.getLogger(__name__)

DAG_NANE = "EKS_TEST"
MAX_ACTIVE_RUNS = 1
TAGS = ["EKS", "TEST"]
SCHEDULE_INTERVAL = None
CLUSTER_NAME = "eks-core"

default_args = {
    "retries": 0,
    "retry_delay": timedelta(seconds=30),
    "start_date": pendulum.today("Europe/Kiev").add(days=-1),
    "email": "",
    "email_on_failure": False,
    "email_on_retry": False,
}


@dag(
    dag_id=DAG_NANE,
    schedule_interval=SCHEDULE_INTERVAL,
    default_args=default_args,
    catchup=False,
    max_active_runs=MAX_ACTIVE_RUNS,
    max_active_tasks=100,
    tags=TAGS,
    render_template_as_native_obj=True,
)
def get_dag():

    start_pod = EksPodOperator(
        task_id="run_pod",
        aws_conn_id="aws_connection_iam",
        cluster_name=CLUSTER_NAME,
        image="registry.k8s.io/pause:2.0",
        cmds=["sh", "-c", "ls"],
        affinity={
            "nodeAffinity": {
                "requiredDuringSchedulingIgnoredDuringExecution": {
                    "nodeSelectorTerms": [
                        {
                            "matchExpressions": [
                                {
                                    "key": "eks.amazonaws.com/nodegroup",
                                    "operator": "In",
                                    "values": ["eksng-spot-apps"],
                                }
                            ]
                        }
                    ]
                }
            }
        },
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "2000m", "memory": "256Mi"},
            limits={
                "cpu": "2000m",
                "memory": "512Mi",
            },
        ),
        labels={"demo": "hello_world"},
        get_logs=True,
        is_delete_operator_pod=True,
    )

dag = get_dag()
