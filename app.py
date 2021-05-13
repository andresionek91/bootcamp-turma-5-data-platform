#!/usr/bin/env python3

from aws_cdk import core
from data_platform.data_lake.stack import DataLakeStack
from data_platform.common_stack import CommonStack
from data_platform.kinesis.stack import KinesisStack
from data_platform.dms.stack import DmsStack

app = core.App()
data_lake_stack = DataLakeStack(app)
commom_stack = CommonStack(app)
kinesis_stack = KinesisStack(app, data_lake_raw_bucket=data_lake_stack.data_lake_raw_bucket)
dms_stack = DmsStack(app, commom_stack=commom_stack, data_lake_raw_bucket=data_lake_stack.data_lake_raw_bucket)
app.synth()
