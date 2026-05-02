#!/usr/bin/env python3

from aws_cdk import App
from stack import FinanalyticsStack


app = App()

FinanalyticsStack(app, "FinAnalyticsStack")

app.synth()
