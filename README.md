# Finanalytics end to end POC project 

## AWS services

This project utilizes the following AWS services
- AWS CDK / CloudFormation
- S3 
- Lambda
- Glue 
- IAM 
- Redshift 
- ECR
- MWAA

## Development tools 

Development tools used to create the data pipeline 
- `dbt` for data modeling on DWH layer 
- `PySpark` for data exploration 
- `neovim` for local data exploration development in the Glue docker environment, can be used to prepare a pure PySpark ETL process instead of Glue Visual ETL
- `airflow` is used to orchestrate the data flow
- `langchain` is an agent based extraction of incomplete data 
- `boto3` AWS python SDK
- `cdk` for deployment

## Setup

### Requirements 

- AWS CDK CLI
- AWS CLI (optional)
- uv (python project manager)

### Preparation 

Clone repo
```sh
git clone https://github.com/x2nur/end2end-finanalytics.github
cd end2end-finanalytics/cdk_deployment
```

Install dependencies
```sh
uv sync
```

Init cdk if cdk hasn't been used before
CDK bootstrap requires **AdministratorAccess** rights (managed policy)
```sh
cdk --profile YOUR_AWS_PROFILE bootstrap 
```

### Deployment 
Deploy
```sh
cdk --profile YOUR_AWS_PROFILE deploy

```

### Uninstallation 
```sh
cdk --profile YOUR_AWS_PROFILE destroy
```

## Airflow data pipeline
<img src="/img/airflow-graph.png" width=70%>


## Usage
