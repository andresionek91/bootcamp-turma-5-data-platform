from aws_cdk import core
from aws_cdk import (
    aws_kinesisfirehose as firehose,
    aws_iam as iam,
)
from data_platform.active_environment import active_environment
from data_platform.data_lake.base import BaseDataLakeBucket


class RawKinesisRole(iam.Role):
    def __init__(
        self,
        scope: core.Construct,
        data_lake_raw_bucket: BaseDataLakeBucket,
        **kwargs,
    ) -> None:
        self.deploy_env = active_environment
        self.data_lake_raw_bucket = data_lake_raw_bucket
        super().__init__(
            scope,
            id=f"iam-{self.deploy_env.value}-data-lake-raw-firehose-role",
            assumed_by=iam.ServicePrincipal("firehose.amazonaws.com"),
            description="Role to allow Kinesis to save data to data lake raw",
        )
        self.add_policy()

    def add_policy(self):
        policy = iam.Policy(
            self,
            id=f"iam-{self.deploy_env.value}-data-lake-raw-firehose-policy",
            policy_name=f"iam-{self.deploy_env.value}-data-lake-raw-firehose-policy",
            statements=[
                iam.PolicyStatement(
                    actions=[
                        "s3:AbortMultipartUpload",
                        "s3:GetBucketLocation",
                        "s3:GetObject",
                        "s3:ListBucket",
                        "s3:ListBucketMultipartUploads",
                        "s3:PutObject",
                    ],
                    resources=[
                        self.data_lake_raw_bucket.bucket_arn,
                        f"{self.data_lake_raw_bucket.bucket_arn}/*",
                    ],
                )
            ],
        )
        self.attach_inline_policy(policy)

        return policy


class KinesisStack(core.Stack):
    def __init__(
        self,
        scope: core.Construct,
        data_lake_raw_bucket: BaseDataLakeBucket,
        **kwargs,
    ) -> None:
        self.deploy_env = active_environment
        self.data_lake_raw_bucket = data_lake_raw_bucket
        super().__init__(scope, id=f"{self.deploy_env.value}-kinesis-stack", **kwargs)

        self.atomic_events = firehose.CfnDeliveryStream(
            self,
            id=f"firehose-{self.deploy_env.value}-raw-delivery-stream",
            delivery_stream_name=f"firehose-{self.deploy_env.value}-raw-delivery-stream",
            delivery_stream_type="DirectPut",
            extended_s3_destination_configuration=self.s3_config,
        )

    @property
    def s3_config(self):
        return firehose.CfnDeliveryStream.ExtendedS3DestinationConfigurationProperty(
            bucket_arn=self.data_lake_raw_bucket.bucket_arn,
            compression_format="GZIP",
            error_output_prefix="bad_records",
            prefix="atomic_events/landing_date=!{timestamp:yyyy}-!{timestamp:MM}-!{timestamp:dd}/",
            buffering_hints=firehose.CfnDeliveryStream.BufferingHintsProperty(
                interval_in_seconds=60, size_in_m_bs=1
            ),
            role_arn=self.kinesis_role.role_arn,
        )

    @property
    def kinesis_role(self):
        return RawKinesisRole(
            self,
            deploy_env=self.deploy_env,
            data_lake_raw_bucket=self.data_lake_raw_bucket,
        )