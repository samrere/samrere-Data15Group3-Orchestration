import json
import math
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.email import EmailOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.utils.task_group import TaskGroup


def collect_lambda_results(task_ids, **kwargs):
    ti = kwargs['ti']
    results = []
    for task_id in task_ids:
        result = ti.xcom_pull(task_ids=task_id)
        results.append(f"Task {task_id} output: {result}")
    return "\n".join(results)


def validity_check_starter(**kwargs):
    s3_hook = S3Hook(aws_conn_id=None)
    file_content = s3_hook.read_key("raw/job_id.csv", kwargs['bucket'])
    df_jobs = file_content.split('\n')[1:]  # removes header job_id
    df_jobs = [job for job in df_jobs if job]  # removes empty

    # Calculate number of workers needed
    total_jobs = len(df_jobs)
    jobs_limit_per_worker = 120
    num_workers = math.ceil(total_jobs / jobs_limit_per_worker)
    jobs_per_lambda = math.ceil(total_jobs / num_workers)

    print(f"Total jobs: {total_jobs}")
    print(f"Number of workers needed: {num_workers}")
    print(f"Jobs per worker: {jobs_per_lambda}")

    worker_configs = []
    for j in range(num_workers):
        start_idx = j * jobs_per_lambda
        end_idx = min((j + 1) * jobs_per_lambda, total_jobs)
        worker_configs.append((start_idx, end_idx))
        print(f"Worker {j}: jobs {start_idx} to {end_idx}")

    return {
        "worker_configs": worker_configs,
        "total_jobs": total_jobs,
        "num_workers": num_workers,
        "jobs_per_worker": jobs_per_lambda
    }


def process_lambda_tasks(**kwargs):
    ti = kwargs["ti"]
    output = ti.xcom_pull(task_ids="split_work")
    worker_configs = output["worker_configs"]
    Variable.set(key='worker_configs', value=worker_configs, serialize_json=True)


datalake_bucket = "data15group3-job-data-lake"
timestamp = datetime.now().isoformat()

with DAG(
        'linkedin_jobs_search',
        default_args={
            'owner': 'airflow',
            'depends_on_past': False,
            'email': [Variable.get('my_email')],
            'email_on_failure': True,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='Search LinkedIn jobs for different roles',
        schedule_interval=None,  # Set to None for manual triggering
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=['linkedin', 'jobs'],
) as dag:
    start = EmptyOperator(task_id='start')
    prev_task = start
    task_ids = []
    # Create tasks for each keyword
    for i, keyword in enumerate(Variable.get("job_keywords", deserialize_json=True)):
        if i > 0:
            # Add wait task between searches
            wait_task = TimeDeltaSensor(
                task_id=f'wait_{i}',
                delta=timedelta(minutes=5)
            )
            prev_task >> wait_task
            prev_task = wait_task

        search_task = LambdaInvokeFunctionOperator(
            task_id=f'search_{keyword.replace(" ", "_").lower()}_jobs',
            function_name='airflow-testJob',
            payload=json.dumps({
                "keyword": keyword,
                "timestamp": timestamp
            }),
            aws_conn_id=None,
            log_type='Tail'
        )
        task_ids.append(search_task.task_id)
        prev_task >> search_task
        prev_task = search_task

    delete_file_task = S3DeleteObjectsOperator(
        task_id="delete_existing_invalid_jobs",
        bucket=datalake_bucket,
        keys='raw/invalid_jobs.csv',
        aws_conn_id=None,
    )

    split_work = PythonOperator(
        task_id='split_work',
        python_callable=validity_check_starter,
        op_kwargs={'bucket': datalake_bucket},
    )

    preparation_task = PythonOperator(
        task_id='preparation_task',
        python_callable=process_lambda_tasks)

    # https://stackoverflow.com/questions/66820948/create-dynamic-workflows-in-airflow-with-xcom-value
    config = Variable.get('worker_configs', default_var=[[0, 1]], deserialize_json=True)
    # https://stackoverflow.com/questions/52741536/running-airflow-tasks-dags-in-parallel
    # in airflow.cfg, change executor = SequentialExecutor to LocalExecutor
    with TaskGroup("worker_tasks") as worker_tasks:
        for i, (start_index, end_index) in enumerate(config):
            task = LambdaInvokeFunctionOperator(
                task_id=f"invoke_lambda_worker_{i}",
                function_name="ValidityCheck-worker",  # Replace with your Lambda function name
                payload=json.dumps({
                    "start_index": start_index,
                    "end_index": end_index,
                }),
                aws_conn_id=None,
                log_type='Tail',
            )
            task_ids.append(task.task_id)

    ValidityCheck_collector = LambdaInvokeFunctionOperator(
        task_id=f"ValidityCheck_collector",
        function_name="ValidityCheck-collector",  # Replace with your Lambda function name
        aws_conn_id=None,
        log_type='Tail'
    )
    task_ids.append(ValidityCheck_collector.task_id)

    collect_results_task = PythonOperator(
        task_id='collect_results',
        python_callable=collect_lambda_results,
        op_kwargs={'task_ids': task_ids},
        provide_context=True
    )

    send_success_email_task = EmailOperator(
        task_id='send_email',
        to=Variable.get('my_email'),
        subject='Data15Group3 workflow - Successful',
        html_content="{{ task_instance.xcom_pull(task_ids='collect_results') }}",
    )

    (
            prev_task
            >> delete_file_task
            >> split_work
            >> preparation_task
            >> worker_tasks
            >> ValidityCheck_collector
            >> collect_results_task
            >> send_success_email_task
    )
