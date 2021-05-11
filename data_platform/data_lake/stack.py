from aws_cdk import core
from aws_cdk import (
    aws_s3 as s3,
)
from data_platform.data_lake.base import BaseDataLakeBucket, DataLakeLayer

from data_platform.active_environment import active_environment


class DataLakeStack(core.Stack):
    def __init__(self, scope: core.Construct, **kwargs) -> None:
        self.deploy_env = active_environment
        super().__init__(scope, id=f'{self.deploy_env.value}-data-lake-stack', **kwargs)

        self.data_lake_raw_bucket = BaseDataLakeBucket(
            self,
            layer=DataLakeLayer.RAW
        )

        self.data_lake_raw_bucket.add_lifecycle_rule(
            transitions=[
                s3.Transition(
                    storage_class=s3.StorageClass.INTELLIGENT_TIERING,
                    transition_after=core.Duration.days(90)
                ),
                s3.Transition(
                    storage_class=s3.StorageClass.GLACIER,
                    transition_after=core.Duration.days(360)
                )
            ],
            enabled=True
        )

        self.data_lake_raw_staged = BaseDataLakeBucket(
            self,
            layer=DataLakeLayer.STAGED
        )

        self.data_lake_raw_curated = BaseDataLakeBucket(
            self,
            layer=DataLakeLayer.CURATED
        )