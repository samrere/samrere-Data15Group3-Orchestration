import json
from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.email import EmailOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.operators.step_function import (
    StepFunctionStartExecutionOperator,
    StepFunctionGetExecutionOutputOperator
)
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.snowflake.operators.snowflake import SQLExecuteQueryOperator
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.utils.task_group import TaskGroup

from sqls import *
from vectordb_utils import *

datalake_bucket = "data15group3-job-data-lake"
current_date = datetime.now(timezone.utc)
year = current_date.strftime('%Y')
month = current_date.strftime('%m')
day = current_date.strftime('%d')


####################################################################################################
# current_date = datetime(2025, 1, 5, 15, 30, tzinfo=timezone.utc)
# year, month, day = '2025', '01', '05'
####################################################################################################

def add_jobs_task(**kwargs):
    current_date = kwargs['current_date']
    pipeline = VectorStorePipeline(
        s3_bucket=datalake_bucket,
        openai_api_key=Variable.get("OPENAI_API_KEY"),
        pinecone_api_key=Variable.get("PINECONE_API_KEY"),
        pinecone_index_name="de15-jd",
    )
    return pipeline.add_entries(current_date)


def delete_jobs_task():
    pipeline = VectorStorePipeline(
        s3_bucket=datalake_bucket,
        openai_api_key=Variable.get("OPENAI_API_KEY"),
        pinecone_api_key=Variable.get("PINECONE_API_KEY"),
        pinecone_index_name="de15-jd",
    )
    return pipeline.delete_entries()


def collect_lambda_results(task_ids, **kwargs):
    ti = kwargs['ti']
    # Start with HTML table structure
    results = ['<table border="1" style="border-collapse: collapse; width: 100%;">']
    # Add table headers
    results.append('<tr><th style="padding: 8px; text-align: left;">Task ID</th>'
                   '<th style="padding: 8px; text-align: left;">Output</th></tr>')
    # Loop through task IDs and add rows
    for task_id in task_ids:
        result = ti.xcom_pull(task_ids=task_id)
        result = result if result else "No result"  # Handle missing results
        # Add row for each task
        results.append(f'<tr>'
                       f'<td style="padding: 8px;">{task_id}</td>'
                       f'<td style="padding: 8px;">{result}</td>'
                       f'</tr>')
    # Close the table
    results.append('</table>')
    # Join all HTML parts into a single string
    return "".join(results)


with DAG(
        'linkedin_jobs_search',
        default_args={
            'owner': 'airflow',
            'depends_on_past': False,
            'email': Variable.get('my_email'),
            'email_on_failure': True,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=3),
        },
        description='Search LinkedIn jobs for different roles',
        schedule_interval=None,  # Set to None for manual triggering
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=['linkedin', 'jobs'],
) as dag:
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    prev_task = start
    task_ids = []
    for i, keyword in enumerate(Variable.get("job_keywords", deserialize_json=True)):
        if i > 0:
            # Add wait task between searches
            wait_task = TimeDeltaSensor(
                task_id=f'wait_{i}',
                delta=timedelta(minutes=5)
            )
            prev_task >> wait_task
            prev_task = wait_task

        # https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/operators/lambda.html#invoke-an-aws-lambda-function
        search_task = LambdaInvokeFunctionOperator(
            task_id=f'search_{keyword.replace(" ", "_").lower()}_jobs',
            function_name='linkedin_scraper',
            payload=json.dumps({
                "keyword": keyword
            }),
            invocation_type='Event',
            aws_conn_id=None,
            log_type='Tail'
        )
        task_ids.append(search_task.task_id)
        prev_task >> search_task
        prev_task = search_task

    wait_for_avro_file = S3KeySensor(
        task_id='wait_for_avro_file',
        bucket_name=datalake_bucket,
        bucket_key=f'raw/{year}/{month}/{day}/Data_Engineer/Data_Engineer-{current_date.strftime("%Y%m%d")}.avro',
        aws_conn_id=None,
        deferrable=True
    )
    prev_task >> wait_for_avro_file
    prev_task = wait_for_avro_file

    with TaskGroup("check_job_validity_tasks") as check_job_validity_tasks:
        start_step_function = StepFunctionStartExecutionOperator(
            task_id='start_step_function',
            state_machine_arn=Variable.get("ValidityChecker_Step_Function_Arn"),
            aws_conn_id=None,
            deferrable=True,
        )

        get_step_function_output = StepFunctionGetExecutionOutputOperator(
            task_id='get_step_function_output',
            execution_arn="{{ task_instance.xcom_pull(task_ids='check_job_validity_tasks.start_step_function') }}",
            aws_conn_id=None,
        )

        task_ids.append(get_step_function_output.task_id)
        start_step_function >> get_step_function_output

    with TaskGroup("add_new_job_tasks") as add_new_job_tasks:
        truncate_stg_job = SQLExecuteQueryOperator(
            task_id='truncate_stg_job',
            conn_id='snowflake_conn',
            sql=TRUNCATE_STG_JOB,
        )
        load_stg_jobs = SQLExecuteQueryOperator(
            task_id='load_stg_jobs',
            conn_id='snowflake_conn',
            sql=COPY_TO_STG_JOBS.format(year=year, month=month, day=day,
                                        fn=f"Data_Engineer-{current_date.strftime('%Y%m%d')}.avro"),
        )
        task_ids.extend([truncate_stg_job.task_id, load_stg_jobs.task_id])
        truncate_stg_job >> load_stg_jobs

        add_doc_to_pinecone = PythonOperator(
            task_id='add_new_jobs_to_pinecone_task',
            python_callable=add_jobs_task,
            op_kwargs={"current_date": current_date},
            provide_context=True
        )
        task_ids.append(add_doc_to_pinecone.task_id)

    with TaskGroup("remove_invalid_job_tasks") as remove_invalid_job_tasks:
        truncate_stg_valid_job_id = SQLExecuteQueryOperator(
            task_id='truncate_stg_valid_job_id',
            conn_id='snowflake_conn',
            sql=TRUNCATE_STG_VALID_JOB_ID,
        )

        load_stg_valid_job_id = SQLExecuteQueryOperator(
            task_id='load_stg_valid_job_id',
            conn_id='snowflake_conn',
            sql=COPY_TO_STG_VALID_JOB_ID,
        )
        task_ids.extend([truncate_stg_valid_job_id.task_id, load_stg_valid_job_id.task_id])
        truncate_stg_valid_job_id >> load_stg_valid_job_id

        truncate_stg_invalid_job_id = SQLExecuteQueryOperator(
            task_id='truncate_stg_invalid_job_id',
            conn_id='snowflake_conn',
            sql=TRUNCATE_STG_INVALID_JOB_ID,
        )

        load_stg_invalid_job_id = SQLExecuteQueryOperator(
            task_id='load_stg_invalid_job_id',
            conn_id='snowflake_conn',
            sql=COPY_TO_STG_INVALID_JOB_ID,
        )
        task_ids.extend([truncate_stg_invalid_job_id.task_id, load_stg_invalid_job_id.task_id])
        truncate_stg_invalid_job_id >> load_stg_invalid_job_id

        remove_doc_from_pinecone = PythonOperator(
            task_id='remove_invalid_jobs_from_pinecone_task',
            python_callable=delete_jobs_task,
            provide_context=True
        )
        task_ids.append(remove_doc_from_pinecone.task_id)

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

    prev_task >> add_new_job_tasks

    (
            prev_task
            >> check_job_validity_tasks
            >> remove_invalid_job_tasks
            >> collect_results_task
            >> send_success_email_task
            >> end
    )
