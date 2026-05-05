import aws_cdk 
from aws_cdk import (
    Stack, 
    Duration, 
    RemovalPolicy, 
    aws_lambda, 
    aws_ecr_assets, 
    aws_s3,
    aws_s3_deployment,
    aws_iam,
    aws_ec2 as aws_ec2,
    aws_secretsmanager,
    aws_redshiftserverless as redshift
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


        # ===== Redshift Serverless Namespace =====

        redshift_admin_secret = aws_secretsmanager.Secret(
            self, "RedshiftAdminSecret",
            secret_name="finanalytics-admin-password",
        )
        
        redshift_role = aws_iam.Role(
            self, "RedshiftRole",
            assumed_by=aws_iam.ServicePrincipal("redshift-serverless.amazonaws.com"), #type:ignore
        )

        # TODO: check output cf template for a secret value 
        redshift_namespace = redshift.CfnNamespace(
            self, "FinanalyticsRedshiftNamespace",
            namespace_name="finanalytics",
            admin_username="admin",
            admin_user_password=redshift_admin_secret.secret_value.unsafe_unwrap(),
            db_name='finanalytics',
            iam_roles=[ redshift_role ]
        )

        # for copy 
        buck.grant_read(redshift_role)

        
        # ===== Security Group for Redshift Workgroup =====
        redshift_sg = aws_ec2.SecurityGroup(
            self, "RedshiftSecurityGroup",
            description="Security group for Redshift Serverless finanalytics workgroup",
            vpc=aws_ec2.Vpc.from_lookup(self, "VPC", is_default=True),
            allow_all_outbound=True,
        )
        # redshift_sg.add_ingress_rule(aws_ec2.Peer.any_ipv4(), aws_ec2.Port.tcp(5439))  

        # ===== Redshift Serverless Workgroup =====
        redshift_workgroup = redshift.CfnWorkgroup(
            self, "FinanalyticsRedshiftWorkgroup",
            workgroup_name="finanalytics",
            namespace_name='finanalytics',
            base_capacity=4,
            max_capacity=8,
            publicly_accessible=False,
            security_group_ids=[redshift_sg.security_group_id],
        )
        redshift_workgroup.add_dependency(redshift_namespace)
        
        # TODO: add mwaa service resources 
