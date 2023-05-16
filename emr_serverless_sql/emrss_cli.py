import click

from .emr_serverless import Session


@click.command()
@click.option("--application-id", default=None, help="Application ID", required=True)
@click.option("--job-role-arn", default=None, help="Job Role ARN", required=True)
@click.option("--s3-bucket", default=None, help="S3 bucket", required=True)
@click.option(
    "--file",
    "-f",
    default=None,
    help="SQL file to execute",
    type=click.Path(exists=True, dir_okay=False),
)
@click.argument("sql_statement", required=False)
def run(application_id, job_role_arn, s3_bucket, sql_statement, file):
    if file and sql_statement:
        raise click.UsageError("Cannot use --file and query string at the same time.")
    if not file and not sql_statement:
        raise click.UsageError("Must specify either --file or query string.")
    session = Session(application_id, job_role_arn, s3_bucket)
    session.start_application()
    jobrunid = (
        session.submit_sql(sql_statement)
        if sql_statement
        else session.submit_sql_file(file)
    )
    output = session.fetch_driver_log(jobrunid)
    print(output)


if __name__ == "__main__":
    run()
