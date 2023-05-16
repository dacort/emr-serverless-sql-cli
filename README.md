# EMR Serverless SQL

An experimental tool for running SQL on EMR Serverless.

Written primarily to scratch an itch, this tool is not recommended for production use-cases.

## Installing

Install and update using pip:

```bash
pip install -U emrss
```

## Running

`emrss` assumes you have a pre-existing EMR Serverless application, IAM job role, and S3 bucket where artifacts will be stored.

You can run simple commands by providing a query string.

```bash
emrss \
    --application-id $APPLICATION_ID \
    --job-role-arn $JOB_ROLE_ARN \
    --s3-bucket $S3_BUCKET \
    "show tables"
```

Or you can also provide a SQL file using the `-f` parameter.

```bash
emrss \
    --application-id $APPLICATION_ID \
    --job-role-arn $JOB_ROLE_ARN \
    --s3-bucket $S3_BUCKET \
    -f script.sql
```
