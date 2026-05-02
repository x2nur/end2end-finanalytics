#!/usr/bin/env python3

from aws_cdk import App
from stack import FinanalyticsStack


app = App()

# prod = aws_cdk.Environment(account="", region="")
# dev = aws_cdk.Environment(account="", region="")

# pass env var to env parameter of Stack
FinanalyticsStack(app, "FinAnalyticsStack")

app.synth()
