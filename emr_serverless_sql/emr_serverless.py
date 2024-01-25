import gzip
import os
import sys
import tempfile
from string import Template

import boto3
from botocore.exceptions import ClientError
from botocore.config import Config

from emr_serverless_sql import console_log


class Session:
    def __init__(self, application_id, job_role, s3_bucket, region):
        self.application_id = application_id
        self.job_role = job_role
        self.s3_bucket = s3_bucket
        self.client = boto3.client("emr-serverless", config=Config(region_name=region))
        self.s3_client = boto3.client("s3")

    def start_application(self):
        console_log(f"Starting application: {self.application_id}")
        self.client.start_application(applicationId=self.application_id)

    def submit_sql_file(self, filename):
        # We need to upload the SQL script to S3
        target_path = f"tmp/{os.path.basename(filename)}"
        try:
            self.s3_client.upload_file(filename, self.s3_bucket, target_path)
        except ClientError as e:
            console_log(f"error uploading file to s3: {e}")
            sys.exit(1)

        script_location = f"s3://{self.s3_bucket}/{target_path}"
        log_location = f"s3://{self.s3_bucket}/logs"
        job_driver = {
            "sparkSubmit": {
                "entryPoint": script_location,
                "sparkSubmitParameters": f"--conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory --class org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver -f",
            }
        }

        return self._submit_job_run("sql-runner", job_driver, log_location)

    def submit_notebook_file(self, filename):
        # Convert the notebook to a script and upload it to S3
        import nbformat
        from nbconvert import PythonExporter

        def convertNotebook(notebookPath, modulePath):
            with open(notebookPath) as fh:
                nb = nbformat.reads(fh.read(), nbformat.NO_CONVERT)

            exporter = PythonExporter()
            source, meta = exporter.from_notebook_node(nb)

            with open(modulePath, "w+") as fh:
                fh.writelines(source)

        target_path = f"tmp/{os.path.basename(filename.replace('.ipynb', '.py'))}"
        filepath = tempfile.mktemp()
        convertNotebook(filename, filepath)
        try:
            self.s3_client.upload_file(filepath, self.s3_bucket, target_path)
        except ClientError as e:
            console_log(f"error uploading file to s3: {e}")
            sys.exit(1)

        script_location = f"s3://{self.s3_bucket}/{target_path}"
        log_location = f"s3://{self.s3_bucket}/logs"
        job_driver = {
            "sparkSubmit": {
                "entryPoint": script_location,
                "sparkSubmitParameters": f"--conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory",
            }
        }

        return self._submit_job_run("notebook-runner", job_driver, log_location)

    def submit_sql(self, sql):
        # Create a placeholder job_driver variable
        job_driver = {}

        # If the SQL is less than 256 characters, we can submit it directly to the application.
        # Otherwise, we need to create a temporary file with our SQL and upload it to S3.
        if len(sql) < 256:
            job_driver = {
                "sparkSubmit": {
                    "entryPoint": sql,
                    "sparkSubmitParameters": "--conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory --class org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver -e",
                }
            }
        else:
            with open(
                os.path.join(os.path.dirname(os.path.abspath(__file__)), "sql_template.py")
            ) as f:
                query_file = Template(f.read()).substitute(query=sql.replace('"', '\\"'))
                self.s3_client.put_object(
                    Body=query_file, Bucket=self.s3_bucket, Key="tmp/emrss-sql_template.py"
                )

            job_driver = {
                "sparkSubmit": {
                    "entryPoint": f"s3://{self.s3_bucket}/tmp/emrss-sql_template.py",
                    "sparkSubmitParameters": "--conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory ",
                }
            }
        log_location = f"s3://{self.s3_bucket}/logs"

        return self._submit_job_run("sql-runner", job_driver, log_location)

    def _submit_job_run(self, name, job_driver, log_location):
        response = self.client.start_job_run(
            applicationId=self.application_id,
            executionRoleArn=self.job_role,
            name="sql-runner",
            jobDriver=job_driver,
            configurationOverrides={
                "monitoringConfiguration": {
                    "s3MonitoringConfiguration": {
                        "logUri": log_location,
                    }
                }
            },
        )

        job_run_id = response.get("jobRunId")
        console_log(f"Job submitted: {job_run_id}")

        job_done = False
        wait = True
        while wait and not job_done:
            jr_response = self.get_job_run(job_run_id)
            job_done = jr_response.get("state") in [
                "SUCCESS",
                "FAILED",
                "CANCELLING",
                "CANCELLED",
            ]

        return job_run_id

    def get_job_run(self, job_run_id: str) -> dict:
        response = self.client.get_job_run(
            applicationId=self.application_id, jobRunId=job_run_id
        )
        return response.get("jobRun")

    def fetch_driver_log(self, job_run_id: str, log_type: str = "stdout") -> str:
        """
        Access the specified `log_type` Driver log on S3 and return the full log string.
        """
        s3_client = boto3.client("s3")
        file_location = f"logs/applications/{self.application_id}/jobs/{job_run_id}/SPARK_DRIVER/{log_type}.gz"
        console_log(f"Fetching {log_type} from s3://{self.s3_bucket}/{file_location}")
        try:
            response = s3_client.get_object(Bucket=self.s3_bucket, Key=file_location)
            file_content = gzip.decompress(response["Body"].read()).decode("utf-8")
        except s3_client.exceptions.NoSuchKey:
            file_content = ""
        return str(file_content)
