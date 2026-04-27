import aws_cdk as cdk
from aws_cdk import (
    Stack,
    aws_lambda,
    aws_ecr,
)
from constructs import Construct

class FinanalyticsStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ===== Missing zip codes lamda =====
        
        missing_zipcodes_resolver_repo = aws_ecr.Repository.from_repository_name(
            self, 
            "MissingZipcodesResolverECRRepo", 
            "missing-zipcodes-resolver"
        )

        missing_zipcodes_resolver_image = aws_lambda.DockerImageCode.from_ecr(
            repository=missing_zipcodes_resolver_repo,
            tag="0.7",
        )

        missing_zipcodes_resolver_lambda = aws_lambda.DockerImageFunction(
            self,
            "MissingZipcodesResolverLambda",
            code=missing_zipcodes_resolver_image,
            memory_size=512,
            timeout=cdk.Duration.seconds(180),
        )
