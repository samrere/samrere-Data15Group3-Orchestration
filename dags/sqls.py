# SQL commands
TRUNCATE_STG_JOB = "TRUNCATE TABLE raw_jobs_data;"

COPY_TO_STG_JOBS = """
COPY INTO raw_jobs_data (
    job_id, title, company, location, 
    description, employment_type, seniority_level, workplace_type,
    industries, job_functions, skills, job_url, 
    apply_url, reposted, posted_time, expire_time,
    ingestion_timestamp, posting_date
)
FROM (
  SELECT 
    $1:job_id::STRING,
    $1:title::STRING,
    $1:company::STRING,
    $1:location::STRING,
    $1:description::STRING,
    $1:employment_type::STRING,
    $1:seniority_level::STRING,
    $1:workplace_type::STRING,
    $1:industries::ARRAY,
    $1:job_functions::ARRAY,
    $1:skills::ARRAY,
    $1:job_url::STRING,
    $1:apply_url::STRING,
    $1:reposted::BOOLEAN,
    $1:posted_time::NUMBER,
    $1:expire_time::NUMBER,
    CURRENT_TIMESTAMP(),
    TO_DATE(TO_VARCHAR($1:posted_time))
  FROM @JOB_DATA_STAGE/{year}/{month}/{day}/Data_Engineer/{fn}
)
FILE_FORMAT = JOB_POSTING_AVRO_FORMAT;
"""


TRUNCATE_STG_VALID_JOB_ID = "TRUNCATE TABLE valid_job_ids;"

COPY_TO_STG_VALID_JOB_ID = """
COPY INTO valid_job_ids (job_id)
FROM @JOB_DATA_STAGE/job_id.csv
FILE_FORMAT = JOB_ID_CSV_FORMAT;
"""

TRUNCATE_STG_INVALID_JOB_ID = "TRUNCATE TABLE invalid_job_ids;"

COPY_TO_STG_INVALID_JOB_ID = """
COPY INTO invalid_job_ids (job_id)
FROM @JOB_DATA_STAGE/invalid_jobs.csv
FILE_FORMAT = JOB_ID_CSV_FORMAT;
"""
