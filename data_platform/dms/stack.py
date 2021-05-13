from aws_cdk import core
from aws_cdk import (
    aws_iam as iam,
    aws_dms as dms,
    aws_ec2 as ec2,
)

import json

from common_stack import CommonStack
from data_platform.data_lake.base import BaseDataLakeBucket


class RawDMSRole(iam.Role):
    def __init__(
        self,
        scope: core.Construct,
        data_lake_raw_bucket: BaseDataLakeBucket,
        **kwargs,
    ) -> None:
        self.deploy_env = scope.deploy_env
        self.data_lake_raw_bucket = data_lake_raw_bucket
        super().__init__(
            scope,
            id=f"iam-{self.deploy_env.value}-data-lake-raw-dms-role",
            assumed_by=iam.ServicePrincipal("dms.amazonaws.com"),
            description="Role to allow DMS to save data to data lake raw",
        )
        self.add_policy()

    def add_policy(self):
        policy = iam.Policy(
            self,
            id=f"iam-{self.deploy_env.value}-data-lake-raw-dms-policy",
            policy_name=f"iam-{self.deploy_env.value}-data-lake-raw-dms-policy",
            statements=[
                iam.PolicyStatement(
                    actions=[
                        "s3:PutObjectTagging",
                        "s3:DeleteObject",
                        "s3:ListBucket",
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


class OrdersDMS(dms.CfnReplicationTask):
    def __init__(
        self,
        scope: core.Construct,
        common_stack: CommonStack,
        data_lake_raw_bucket: BaseDataLakeBucket,
        **kwargs,
    ) -> None:
        self.data_lake_raw_bucket = data_lake_raw_bucket
        self.common_stack = common_stack
        self.deploy_env = scope.deploy_env
        self.rds_endpoint = dms.CfnEndpoint(
            scope,
            f"dms-{self.deploy_env.value}-orders-rds-endpoint",
            endpoint_type="source",
            endpoint_identifier=f"dms-source-{self.deploy_env.value}-orders-rds-endpoint",
            engine_name="postgres",
            password=core.CfnDynamicReference(
                core.CfnDynamicReferenceService.SECRETS_MANAGER,
                key=f"{self.common_stack.orders_rds.secret.secret_arn}:SecretString:password",
            ).to_string(),
            username=core.CfnDynamicReference(
                core.CfnDynamicReferenceService.SECRETS_MANAGER,
                key=f"{self.common_stack.orders_rds.secret.secret_arn}:SecretString:username",
            ).to_string(),
            database_name=core.CfnDynamicReference(
                core.CfnDynamicReferenceService.SECRETS_MANAGER,
                key=f"{self.common_stack.orders_rds.secret.secret_arn}:SecretString:dbname",
            ).to_string(),
            port=float(core.CfnDynamicReference(
                core.CfnDynamicReferenceService.SECRETS_MANAGER,
                key=f"{self.common_stack.orders_rds.secret.secret_arn}:SecretString:port",
            ).to_string()),
            server_name=self.common_stack.orders_rds.db_instance_endpoint_address,
            extra_connection_attributes="captureDDLs=Y",
        )

        self.s3_endpoint = dms.CfnEndpoint(
            scope,
            f"dms-{self.deploy_env.value}-orders-s3-endpoint",
            endpoint_type="target",
            engine_name="s3",
            endpoint_identifier=f"dms-target-{self.deploy_env.value}-orders-s3-endpoint",
            extra_connection_attributes="DataFormat=parquet;maxFileSize=131072;timestampColumnName=extracted_at;includeOpForFullLoad=true",
            s3_settings=dms.CfnEndpoint.S3SettingsProperty(
                bucket_name=self.data_lake_raw_bucket.bucket_name,
                bucket_folder="orders",
                compression_type="gzip",
                csv_delimiter=",",
                csv_row_delimiter="\n",
                service_access_role_arn=RawDMSRole(
                    scope, self.data_lake_raw_bucket
                ).role_arn,
            ),
        )

        self.dms_sg = ec2.SecurityGroup(
            scope,
            f"dms-{self.deploy_env.value}-sg",
            vpc=self.common_stack.custom_vpc,
            security_group_name=f"dms-{self.deploy_env.value}-sg",
        )

        self.dms_subnet_group = dms.CfnReplicationSubnetGroup(
            scope,
            f"dms-{self.deploy_env.value}-replication-subnet",
            replication_subnet_group_description="dms replication instance subnet group",
            subnet_ids=[
                subnet.subnet_id
                for subnet in self.common_stack.custom_vpc.private_subnets
            ],
            replication_subnet_group_identifier=f"dms-{self.deploy_env.value}-replication-subnet",
        )

        self.instance = dms.CfnReplicationInstance(
            scope,
            f"dms-replication-instance-{self.deploy_env.value}",
            allocated_storage=100,
            publicly_accessible=False,
            engine_version="3.4.4",
            replication_instance_class="dms.t2.small",
            replication_instance_identifier=f"dms-{self.deploy_env.value}-replication-instance",
            vpc_security_group_ids=[self.dms_sg.security_group_id],
            replication_subnet_group_identifier=self.dms_subnet_group.replication_subnet_group_identifier,
        )

        self.instance.node.add_dependency(self.dms_subnet_group)
        self.instance.node.add_dependency(self.dms_sg)

        super().__init__(
            scope,
            f"{self.deploy_env.value}-dms-task-orders-rds",
            migration_type="full-load-and-cdc",
            replication_task_identifier=f"{self.deploy_env.value}-dms-task-orders-rds",
            replication_instance_arn=self.instance.ref,
            source_endpoint_arn=self.rds_endpoint.ref,
            target_endpoint_arn=self.s3_endpoint.ref,
            table_mappings=json.dumps(
                {
                    "rules": [
                        {
                            "rule-type": "selection",
                            "rule-id": "1",
                            "rule-name": "1",
                            "object-locator": {
                                "schema-name": "%",
                                "table-name": "%",
                            },
                            "rule-action": "include",
                            "filters": [],
                        }
                    ]
                }
            ),
        )


class DmsStack(core.Stack):
    def __init__(
        self,
        scope: core.Construct,
        common_stack: CommonStack,
        data_lake_raw_bucket: BaseDataLakeBucket,
        **kwargs,
    ) -> None:
        self.deploy_env = scope.deploy_env
        self.data_lake_raw_bucket = data_lake_raw_bucket
        super().__init__(scope, id=f"{self.deploy_env.value}-dms-stack", **kwargs)

        self.dms_replication_task = OrdersDMS(self, common_stack, data_lake_raw_bucket)
