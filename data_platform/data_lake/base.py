from enum import Enum
from aws_cdk import core
from aws_cdk import (
    aws_s3 as s3,
)


class DataLakeLayer(Enum):
    RAW = 'raw'
    STAGED = 'staged'
    CURATED = 'curated'


class BaseDataLakeBucket(s3.Bucket):
    def __init__(self, scope: core.Construct, layer: DataLakeLayer, **kwargs):
        self.layer = layer
        self.deploy_env = scope.deploy_env
        self.obj_name = f's3-belisco-turma-5-{self.deploy_env.value}-data-lake-{self.layer.value}'

        super().__init__(
            scope,
            id=self.obj_name,
            bucket_name=self.obj_name,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            versioned=True,
            **kwargs
        )

        self.set_default_lifecycle_rules()


    def set_default_lifecycle_rules(self):
        """
        Sets lifecycle rule by default
        """
        self.add_lifecycle_rule(
            abort_incomplete_multipart_upload_after=core.Duration.days(7),
            enabled=True
        )

        self.add_lifecycle_rule(
            noncurrent_version_transitions=[
                s3.NoncurrentVersionTransition(
                    storage_class=s3.StorageClass.INFREQUENT_ACCESS,
                    transition_after=core.Duration.days(30)
                ),
                s3.NoncurrentVersionTransition(
                    storage_class=s3.StorageClass.GLACIER,
                    transition_after=core.Duration.days(60)
                )
            ]
        )

        self.add_lifecycle_rule(
            noncurrent_version_expiration=core.Duration.days(360)
        )