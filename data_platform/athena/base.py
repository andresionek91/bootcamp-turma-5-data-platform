from aws_cdk import core
from aws_cdk import (
    aws_s3 as s3,
    aws_athena as athena,
)


class BaseAthenaBucket(s3.Bucket):

    def __init__(self, scope: core.Construct, **kwargs) -> None:
        self.deploy_env = scope.deploy_env
        self.obj_name = f's3-belisco-{self.deploy_env.value}-data-lake-athena-results'

        super().__init__(
            scope,
            id=self.obj_name,
            bucket_name=self.obj_name,
            removal_policy=core.RemovalPolicy.DESTROY,
            block_public_access=self.default_block_public_access(),
            encryption=self.default_encryption(),
            versioned=True,
            **kwargs
        )

        self.add_lifecycle_rule(
            expiration=core.Duration.days(60)
        )

    @staticmethod
    def default_block_public_access():
        """
        Block public access by default
        """
        block_public_access = s3.BlockPublicAccess(
            block_public_acls=True,
            block_public_policy=True,
            ignore_public_acls=True,
            restrict_public_buckets=True
        )
        return block_public_access

    @staticmethod
    def default_encryption():
        """
        Enables encryption by default
        """
        encryption = s3.BucketEncryption(s3.BucketEncryption.S3_MANAGED)
        return encryption


class BaseAthenaWorkgroup(athena.CfnWorkGroup):

    def __init__(self, scope: core.Construct, athena_bucket: BaseAthenaBucket, gb_scanned_cutoff_per_query: int, **kwargs) -> None:
        self.gb_scanned_cutoff_per_query = gb_scanned_cutoff_per_query
        self.deploy_env = scope.deploy_env
        self.athena_bucket = athena_bucket
        self.obj_name = f's3-belisco-{self.deploy_env.value}-data-lake-athena-workgroup'
        super().__init__(
            scope,
            id=self.obj_name,
            name=self.obj_name,
            description='Workgroup padrao para execucao de queries',
            recursive_delete_option=True,
            state='ENABLED',
            work_group_configuration=self.default_workgroup_configuration,
            **kwargs
        )

    @property
    def default_workgroup_configuration(self):
        return athena.CfnWorkGroup.WorkGroupConfigurationProperty(
            bytes_scanned_cutoff_per_query=self.bytes_scanned_cutoff_per_query,
            enforce_work_group_configuration=True,
            publish_cloud_watch_metrics_enabled=True,
            result_configuration=self.default_result_configuration
        )

    @property
    def default_result_configuration(self):
        return athena.CfnWorkGroup.ResultConfigurationProperty(
            encryption_configuration=athena.CfnWorkGroup.EncryptionConfigurationProperty(encryption_option='SSE_S3'),
            output_location=f's3://{self.athena_bucket.bucket_name}'
        )

    @property
    def bytes_scanned_cutoff_per_query(self):
        return self.gb_scanned_cutoff_per_query * 1000000000