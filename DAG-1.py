# Import all modules
from airflow import DAG
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# Variable section
BUCKET = "testing_dag_bkt"
PROJECT_ID = "sam-project-12543"
DATASET_NAME_1 = "raw_ds"
DATASET_NAME_2 = "insight_ds"
TABLE_NAME_1 = "employee1"
TABLE_NAME_2 = "departments1"
TABLE_NAME_3 = "empDep_in_all_in_one"
LOCATION = "US"
ARGS = {
    "owner": "sam kale",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "email_on_failure": True,
    "email_on_retry": True,
    "email": ["kalesameer222@gmail.com"],
    "execution_timeout": timedelta(minutes=15),
}
QUERY = f"""
CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_NAME_2}.{TABLE_NAME_3}` AS
SELECT
    e.EmployeeID,
    CONCAT(e.FirstName, ".", e.LastName) AS FullName,
    e.Email,
    e.Salary,
    e.JoinDate,
    d.DepartmentID,
    d.DepartmentName,
    CAST(e.Salary AS INTEGER) * 0.01 as EmpTax
FROM `{PROJECT_ID}.{DATASET_NAME_1}.{TABLE_NAME_1}` e
LEFT JOIN `{PROJECT_ID}.{DATASET_NAME_1}.{TABLE_NAME_2}` d
ON e.DepartmentID = d.DepartmentID
WHERE e.EmployeeID IS NOT NULL
"""
# Define the DAG
with DAG(
    "LEVEL_1_DAG",
    schedule_interval="50 22 11 0 1",
    description="DAG to load data from GCS to BigQuery and create an enriched employee table",
    default_args=ARGS,
    tags=["gcs", "bq", "etl", "data engineering", "alpha"],
) as dag:
    # Define the tasks
    load_emp_csv = GCSToBigQueryOperator(
        task_id="load_emp_csv",
        description="Load employee data from GCS to BigQuery",
        bucket=BUCKET,
        source_objects=["employee.csv"],
        destination_project_dataset_table=f"{DATASET_NAME_1}.{TABLE_NAME_1}",
        schema_fields=[
            {"name": "EmployeeID", "type": "INT64", "mode": "NULLABLE"},
            {"name": "FirstName", "type": "STRING", "mode": "NULLABLE"},
            {"name": "LastName", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Email", "type": "STRING", "mode": "NULLABLE"},
            {"name": "DepartmentID", "type": "INT64", "mode": "NULLABLE"},
            {"name": "Salary", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "JoinDate", "type": "STRING", "mode": "NULLABLE"},
        ],
        write_disposition="WRITE_TRUNCATE",
    )
    load_dep_csv = GCSToBigQueryOperator(
        task_id="load_dep_csv",
        description="Load department data from GCS to BigQuery",
        bucket=BUCKET,
        source_objects=["departments.csv"],
        destination_project_dataset_table=f"{DATASET_NAME_1}.{TABLE_NAME_2}",
        schema_fields=[
            {"name": "DepartmentID", "type": "INT64", "mode": "NULLABLE"},
            {"name": "DepartmentName", "type": "STRING", "mode": "NULLABLE"},
        ],
        write_disposition="WRITE_TRUNCATE",
    )
    insert_query_job = BigQueryInsertJobOperator(
        task_id="insert_query_job",
        configuration={
            "query": {
                "query": QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
        location=LOCATION,
    )
    # Define the dependencies
    (load_emp_csv, load_dep_csv) >> insert_query_job

    #we created DAG1 please use this code and create one CICD pipeline