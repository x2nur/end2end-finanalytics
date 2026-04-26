from aws_cdk import ( 
    Stack 
    , aws_lambda as lmb
)
from constructs import Construct

class FinAnalyticsStack(Stack):

    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)



