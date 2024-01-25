import click

from .emr_serverless import Session


@click.command()
@click.option("--application-id", default=None, help="Application ID", required=True)
@click.option("--job-role-arn", default=None, help="Job Role ARN", required=True)
@click.option("--s3-bucket", default=None, help="S3 bucket", required=True)
@click.option("--region", default=None, help="Region", required=False)
@click.option(
    "--file",
    "-f",
    default=None,
    help="SQL file to execute",
    type=click.Path(exists=True, dir_okay=False),
)
@click.argument("sql_statement", required=False)
def run(application_id, job_role_arn, s3_bucket, region, sql_statement, file):
    if file and sql_statement:
        raise click.UsageError("Cannot use --file and query string at the same time.")
    if not file and not sql_statement:
        raise click.UsageError("Must specify either --file or query string.")
    if file and (not file.endswith(".sql") and not file.endswith(".ipynb")):
        raise click.UsageError("File must be a .sql or .ipynb file.")
    session = Session(application_id, job_role_arn, s3_bucket, region)
    session.start_application()
    jobrunid: str
    if sql_statement:
        jobrunid = session.submit_sql(sql_statement)
    elif file.endswith(".sql"):
        jobrunid = session.submit_sql_file(file)
    elif file.endswith(".ipynb"):
        jobrunid = session.submit_notebook_file(file)
    output = session.fetch_driver_log(jobrunid)
    print(output)


if __name__ == "__main__":
    run()
