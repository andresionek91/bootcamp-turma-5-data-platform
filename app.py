#!/usr/bin/env python3

from aws_cdk import core
from data_platform.data_lake.stack import DataLakeStack

app = core.App()
data_lake_stack = DataLakeStack(app)
app.synth()
