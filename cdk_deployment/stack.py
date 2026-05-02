import aws_cdk 
from aws_cdk import (
    Stack, 
    Duration, 
    RemovalPolicy, 
    aws_lambda, 
    aws_ecr_assets, 
    aws_s3,
    aws_s3_deployment,
    aws_iam
)
import aws_cdk.aws_glue_alpha as glue
from aws_cdk.aws_s3_deployment import Source
from constructs import Construct


class FinanalyticsStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)


        # ===== Missing zip codes lamda =====

        # missing_zipcodes_resolver_repo = aws_ecr.Repository.from_repository_name(
        #     self,
        #     "MissingZipcodesResolverECRRepo",
        #     "missing-zipcodes-resolver"
        # )

        # missing_zipcodes_resolver_image = aws_lambda.DockerImageCode.from_ecr(
        #     repository=missing_zipcodes_resolver_repo,
        #     tag="0.7",
        # )

        miss_zip_resolver_img = aws_lambda.DockerImageCode.from_image_asset(
            directory="../agent",
            platform=aws_ecr_assets.Platform.LINUX_AMD64,
            network_mode=aws_ecr_assets.NetworkMode.HOST,
        )

        missing_zipcodes_resolver_lambda = aws_lambda.DockerImageFunction(
            self,
            "MissingZipcodesResolverLambda",
            code=miss_zip_resolver_img,
            memory_size=512,
            timeout=Duration.seconds(180),
        )

        env = self.node.try_get_context('env')
        is_prod = True if env is not None and env == 'prod' else False


        # ===== S3 Bucket =====

        buck = aws_s3.Bucket(
            self, 
            "FinAnalytics", 
            block_public_access=aws_s3.BlockPublicAccess.BLOCK_ALL,
            auto_delete_objects=not is_prod,
        )
        if not is_prod:
            buck.apply_removal_policy(RemovalPolicy.DESTROY)
    
        buck.grant_read_write(missing_zipcodes_resolver_lambda)


        # ===== Deploy glue etl scripts =====

        aws_s3_deployment.BucketDeployment(
            self,
            "DeployGluePythonFiles",
            sources=[Source.asset("../glue")],
            destination_bucket=buck,
            destination_key_prefix="etl-scripts",
        )


        # ===== IAM Role for Glue =====

        glue_role = aws_iam.Role(
            self, "GlueServiceRole",
            assumed_by=aws_iam.ServicePrincipal("glue.amazonaws.com"), #type:ignore
            managed_policies=[
                aws_iam.ManagedPolicy.from_aws_managed_policy_name("AWSGlueServiceRole")
            ]
        )
        buck.grant_read_write(glue_role)


        # ===== Glue Job: transactions step1 =====

        transactions_step1_glue_job = glue.PySparkEtlJob(
            self, 
            "TransactionsStep1ETLJob",
            job_name="transactions-step1-etl",
            glue_version=glue.GlueVersion.V5_1,
            role=glue_role, #type: ignore
            script=glue.Code.from_bucket(buck, 'etl-scripts/transactions-step1-etl.py'),
            number_of_workers=4, 
            worker_type=glue.WorkerType.G_1X,
            max_concurrent_runs=1,
            timeout=Duration.minutes(30),
        )


        # ===== Glue Job: transactions step2 =====

        transactions_step2_glue_job = glue.PySparkEtlJob(
            self, 
            "TransactionsStep2ETLJob",
            job_name="transactions-step2-etl",
            glue_version=glue.GlueVersion.V5_1,
            role=glue_role, #type: ignore
            script=glue.Code.from_bucket(buck, 'etl-scripts/transactions-step2-etl.py'),
            number_of_workers=2, 
            worker_type=glue.WorkerType.G_1X,
            max_concurrent_runs=1,
            timeout=Duration.minutes(30),
        )


        # ===== Glue Job: users =====

        users_glue_job = glue.PySparkEtlJob(
            self, 
            "UsersETLJob",
            job_name="users-etl",
            glue_version=glue.GlueVersion.V5_1,
            role=glue_role, #type: ignore
            script=glue.Code.from_bucket(buck, 'etl-scripts/users-etl.py'),
            number_of_workers=2, 
            worker_type=glue.WorkerType.G_1X,
            max_concurrent_runs=1,
            timeout=Duration.minutes(30),
        )


        # ===== Glue Job: cards =====

        cards_glue_job = glue.PySparkEtlJob(
            self, 
            "CardsETLJob",
            job_name="cards-etl",
            glue_version=glue.GlueVersion.V5_1,
            role=glue_role, #type: ignore
            script=glue.Code.from_bucket(buck, 'etl-scripts/cards-etl.py'),
            number_of_workers=2, 
            worker_type=glue.WorkerType.G_1X,
            max_concurrent_runs=1,
            timeout=Duration.minutes(30),
        )


        # ===== Glue Job: mcc-codes =====

        mcc_codes_glue_job = glue.PySparkEtlJob(
            self, 
            "MccCodesETLJob",
            job_name="mcc-codes-etl",
            glue_version=glue.GlueVersion.V5_1,
            role=glue_role, #type: ignore
            script=glue.Code.from_bucket(buck, 'etl-scripts/mcc_codes-etl.py'),
            number_of_workers=2, 
            worker_type=glue.WorkerType.G_1X,
            max_concurrent_runs=1,
            timeout=Duration.minutes(30),
        )
