import aws_cdk
from aws_cdk import Stack, Duration, aws_lambda, aws_ecr_assets, aws_s3
from aws_cdk import aws_s3_deployment, aws_iam
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
            buck.apply_removal_policy(aws_cdk.RemovalPolicy.DESTROY)
    
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




