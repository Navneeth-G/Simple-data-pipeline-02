# data_pipeline_project/projects/group_name/class_name/main_pipeline_dag.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
import pendulum
from datetime import timedelta
from pathlib import Path

from data_pipeline_project.pipeline_framework.config_handler import config_handler
from data_pipeline_project.pipeline_framework.airflow_tasks.store_new_and_pass_valid_record import store_new_and_pass_valid_record
from data_pipeline_project.pipeline_framework.airflow_tasks.pre_validation_count_check import pre_validate_counts
from data_pipeline_project.pipeline_framework.airflow_tasks.source_to_stage_transfer import source_to_stage_task
from data_pipeline_project.pipeline_framework.airflow_tasks.stage_to_target_transfer import stage_to_target_task
from data_pipeline_project.pipeline_framework.airflow_tasks.audit_task import audit_task
from data_pipeline_project.pipeline_framework.airflow_tasks.cleanup_stale_locks import cleanup_stale_locks

# Define root project path
root_project_path = str(Path(__file__).parent)

# Load config
final_config = config_handler(
    root_project_path,
    "../../../pipeline_framework/default_record.json",  # Go up to projects, then to pipeline_framework
    "./config.json"                                      # Config file in same directory
)

# Override with Airflow Variables if needed
if Variable.get("SF_USERNAME", None): 
    final_config["sf_creds"]["user"] = Variable.get("SF_USERNAME")
if Variable.get("SF_PASSWORD", None): 
    final_config["sf_creds"]["password"] = Variable.get("SF_PASSWORD")
if Variable.get("ES_HOST", None): 
    final_config["host_name"] = Variable.get("ES_HOST")

def store_and_validate_wrapper(**context):
    dag_run_id = context['dag_run'].dag_id
    result = store_new_and_pass_valid_record(final_config, dag_run_id)
    if result['exit_dag']:
        raise AirflowSkipException("No valid record to process")
    return result['record']

def pre_validation_wrapper(**context):
    record = context['ti'].xcom_pull(task_ids='store_and_validate')
    result = pre_validate_counts(final_config, record)
    if result['exit_dag']:
        raise AirflowSkipException(f"Skip: {result['reason']}")
    return record

def source_to_stage_wrapper(**context):
    record = context['ti'].xcom_pull(task_ids='pre_validation')
    result = source_to_stage_task(final_config, record)
    if not result['success']:
        raise Exception(result['reason'])
    return result['record']

def stage_to_target_wrapper(**context):
    record = context['ti'].xcom_pull(task_ids='source_to_stage')
    result = stage_to_target_task(final_config, record)
    if not result['success']:
        raise Exception(result['reason'])
    return result['record']

def audit_wrapper(**context):
    record = context['ti'].xcom_pull(task_ids='stage_to_target')
    result = audit_task(final_config, record)
    if not result['success']:
        raise Exception(result['reason'])
    return result['record']

def cleanup_wrapper(**context):
    return cleanup_stale_locks(final_config)

default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2025, 1, 1, tz=final_config["timezone"]),
    'email_on_failure': final_config.get('email_on_failure', True),
    'email_on_retry': final_config.get('email_on_retry', False),
    'email': final_config.get('email_list', []),
    'retries': final_config.get('max_retry_task', 1),
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'data_pipeline_main',
    default_args=default_args,
    description='Universal Data Pipeline with Dynamic Config',
    schedule_interval=final_config.get('cron_schedule'),
    catchup=False,
    max_active_runs=1,
    tags=['data', 'pipeline', 'framework']
) as dag:

    store_and_validate = PythonOperator(
        task_id='store_and_validate',
        python_callable=store_and_validate_wrapper
    )

    pre_validation = PythonOperator(
        task_id='pre_validation',
        python_callable=pre_validation_wrapper
    )

    source_to_stage = PythonOperator(
        task_id='source_to_stage',
        python_callable=source_to_stage_wrapper
    )

    stage_to_target = PythonOperator(
        task_id='stage_to_target',
        python_callable=stage_to_target_wrapper
    )

    audit = PythonOperator(
        task_id='audit',
        python_callable=audit_wrapper
    )

    cleanup_stale_locks_task = PythonOperator(
        task_id='cleanup_stale_locks',
        python_callable=cleanup_wrapper,
        trigger_rule='all_done'
    )

    # Dependencies
    store_and_validate >> pre_validation >> source_to_stage >> stage_to_target >> audit >> cleanup_stale_locks_task