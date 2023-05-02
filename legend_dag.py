from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.gitlab_download_operator import GitLabDownloadOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryExecuteQueryOperator,
    BigQueryExtractTableOperator,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGoogleCloudStorageOperator
from airflow.utils.dates import days_ago

from your_script import legend_to_bigquery, bigquery_to_legend
import json
import gitlab
import base64
import re
import git
import shutil

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "legend",
    default_args=default_args,
    description="DAG for processing Employee and Organisation data",
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
)

# Step 1: Extract Employee.pure and Organisation.pure files from GitLab and copy the files to the storage bucket gs://legend_bucket
def retrieve_from_gitlab(filename):

    # private token authentication
    gl = gitlab.Gitlab("https://gitlab.com/igorkleiman", private_token='legend_new')

    gl.auth()
    client = storage.Client()
    bucket = client.get_bucket('gs://legend_bucket')
    blob = bucket.blob(filename)

    # list all projects
    projects = gl.projects.list(all=True)
    for project in projects:
        # Skip projects without branches
        if len(project.branches.list()) == 0:
            continue
        if project.name == "epam":
            branch = project.branches.list()[0].name

            try:
                f = project.files.get(file_path='epam-entities/src/main/pure/entities/'+filename, ref=branch)
            except gitlab.exceptions.GitlabGetError:
                # Skip projects without Dockerfile
                continue

            file_content = base64.b64decode(f.content).decode("utf-8")
            blob.upload_from_string(file_content)

download_employee_pure =  PythonOperator(
    task_id="download_employee_pure",
    python_callable=retrieve_from_gitlab,
    op_args=["Employee.pure"],
    dag=dag,
)

download_organisation_pure = PythonOperator(
    task_id="download_organisation_pure",
    python_callable=retrieve_from_gitlab,
    op_args=["Organisation.pure"],
    dag=dag,
)



# Step 2: Convert Employee.pure and Organisation.pure to employee.json and organisation.json
def parse_pure_class(pure_class):
    class_name_match = re.search(r"class (\w+)", pure_class)
    if not class_name_match:
        return None

    class_name = class_name_match.group(1)
    attribute_matches = re.findall(r"(\w+) : (\w+);", pure_class)
    attributes = {attr_name: attr_type for attr_name, attr_type in attribute_matches}

    return class_name, attributes

def pure_to_bigquery_json(pure_data, filename):
    pure_classes = re.findall(r"class \w+ {[^}]+}", pure_data)
    parsed_classes = [parse_pure_class(pure_class) for pure_class in pure_classes]

    bigquery_schema = []

    for class_name, attributes in parsed_classes:
        schema = {
            "name": class_name,
            "type": "RECORD",
            "fields": [{"name": attr_name, "type": "STRING"} for attr_name in attributes]
        }
        bigquery_schema.append(schema)
    client = storage.Client()
    bucket = client.get_bucket('gs://legend_bucket')
    blob = bucket.blob(filename)
    blob.upload_from_string(json.dumps(bigquery_schema, indent=2))



convert_employee_pure_to_json = PythonOperator(
    task_id="convert_employee_pure_to_json",
    python_callable=pure_to_bigquery_json,
    op_args=["gs://legend_bucket/Employee.pure", "employee.json"],
    dag=dag,
)

convert_organisation_pure_to_json = PythonOperator(
    task_id="convert_organisation_pure_to_json",
    mpython_callable=pure_to_bigquery_json,
    op_args=["gs://legend_bucket/Organisation.pure", "organisation.json"],
    dag=dag,
)



# Step 3: Create tables employee and organisation in the dataset epam
update_employee_schema_json = BigQueryCreateEmptyTableOperator(
    task_id="update_employee_schema_json",
    dataset_id="secure-bonus-282818.legend",
    table_id="Employee",
    gcs_schema_object="gs://legend_bucket/employee.json",
)

update_organisation_schema_json = BigQueryCreateEmptyTableOperator(
    task_id="update_organisation_schema_json",
    dataset_id="secure-bonus-282818.legend",
    table_id="Organisation",
    gcs_schema_object="gs://legend_bucket/organisation.json",
)

# Step 4: Alter table employee by adding a new column "newcolumn"
alter_employee_table= BigQueryInsertJobOperator(
    task_id="alter_employee_table",
    configuration={
        'query': {
            'query': "ALTER TABLE secure-bonus-282818.legend.employee ADD COLUMN newcolumn STRING;",
            'destinationTable': {
                'projectId': "secure-bonus-282818",
                'datasetId': "secure-bonus-282818.legend",
                'tableId': "secure-bonus-282818.legend.Employee"
            },
            'useLegacySql': False,
            'allowLargeResults': True,
        }
    },
    dag=dag
)



# Step 5: Extract tables employee and organisation to Employee.json and Organisation.json
extract_employee_table =  BigQueryToGCSOperator(
    task_id="extract_employee_table",
    source_project_dataset_table="secure-bonus-282818.legend.Employee",
    destination_cloud_storage_uris=["gs://legend_bucket/employee.json"],
)

extract_organisation_table = BigQueryToGCSOperator(
    task_id="extract_employee_table",
    source_project_dataset_table="secure-bonus-282818.legend.Organisation",
    destination_cloud_storage_uris=["gs://legend_bucket/organisation.json"],
)

# Step 6: Convert Employee.json to Employee.pure and Organisation.json to Organisation.pure
def bigquery_field_to_pure_attribute(field):
    name = field['name']
    if field['type'].upper() == 'STRING':
        legend_type = 'String'
    else:
        # Add support for other types here if needed
        legend_type = 'Any'
    return f'{name} : {legend_type};'

def bigquery_record_to_pure_class(record):
    class_name = record['name']
    attributes = [bigquery_field_to_pure_attribute(field) for field in record['fields']]
    attributes_str = '\n  '.join(attributes)
    return f'class {class_name} {{\n  {attributes_str}\n}}'

def bigquery_json_to_pure(json_data, filename):
    records = json.loads(json_data)
    pure_classes = [bigquery_record_to_pure_class(record) for record in records]
    client = storage.Client()
    bucket = client.get_bucket('gs://legend_bucket')
    blob = bucket.blob(filename)
    blob.upload_from_string('\n\n'.join(pure_classes))





convert_employee_json_to_pure = PythonOperator(
    task_id="convert_employee_json_to_pure",
    python_callable=bigquery_json_to_pure,
    op_args=["gs://legend_bucket/employee.json", "Employee.pure"],
    dag=dag,
)

convert_organisation_json_to_pure = PythonOperator(
    task_id="convert_organisation_json_to_pure",
    python_callable=convert_json_to_pure,
    op_args=["/path/to/Organisation.json", "Organisation.pure"],
    dag=dag,
)

# Step 7: Push Employee.pure and Organisation.pure to gitlab project epam

def pushToRepo():
    """GitLab API"""
    gl = gitlab.Gitlab("https://gitlab.com", private_token = 'legend_new', api_version=4)

    gl.auth()

    """Cloning the Base Code"""
    git.Repo.clone_from("igorkleiman/epam","gs://legend_bucket")
    shutil.copy("gs://legend_bucket/Employee.pure", "gs://legend_bucket/epam/entities/src/main/pure/entities")


    """Create new git repo in the new folder"""
    new_repo = git.Repo.init('{}'.format("epam"))

    """Adding all the files to Staged Scenario"""
    new_repo.index.add(['.'])

    """Commit the changes"""
    new_repo.index.commit('final commit.')




push_employee_pure_to_gitlab = PythonOperator(
    task_id="convert_organisation_pure_to_json",
    mpython_callable=pushToRepo,
    dag=dag,
)


download_employee_pure >> download_organisation_pure >> convert_employee_pure_to_json >> convert_organisation_pure_to_json
convert_organisation_pure_to_json >> update_employee_schema_json >> update_organisation_schema_json >> alter_employee_table
alter_employee_table >> extract_employee_table >> extract_organisation_table >> convert_employee_json_to_pure >> convert_organisation_json_to_pure >> push_employee_pure_to_gitlab