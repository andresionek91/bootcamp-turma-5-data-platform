from aws_cdk import core
from aws_cdk import (
    aws_mwaa as mwaa,
    aws_logs as logs,
    aws_ec2 as ec2,
    aws_s3 as s3,
    aws_iam as iam,
    aws_s3_deployment as s3deploy
)

from common_stack import CommonStack
from data_platform.data_lake.base import BaseDataLakeBucket
from data_platform.active_environment import active_environment
import os
from zipfile import ZipFile


def _get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


class AirflowStack(core.Stack):
    def __init__(
        self,
        scope: core.Construct,
        common_stack: CommonStack,
        data_lake_raw_bucket: BaseDataLakeBucket,
        **kwargs,
    ) -> None:
        self.deploy_env = active_environment
        self.common_stack = common_stack
        self.data_lake_raw_bucket = data_lake_raw_bucket
        super().__init__(scope, id=f"{self.deploy_env.value}-airflow-stack", **kwargs)

        self.log_group = logs.LogGroup(
            self,
            id=f"{self.deploy_env.value}-airflow-log-group",
            log_group_name=f"{self.deploy_env.value}-airflow-log-group",
            retention=logs.RetentionDays.THREE_MONTHS,
            removal_policy=core.RemovalPolicy.DESTROY
        )

        self.logging_configuration = mwaa.CfnEnvironment.ModuleLoggingConfigurationProperty(
            cloud_watch_log_group_arn=self.log_group.log_group_arn,
            enabled=True,
            log_level="WARNING"
        )

        self.security_group = ec2.SecurityGroup(
            self,
            f"airflow-{self.deploy_env.value}-sg",
            vpc=self.common_stack.custom_vpc,
            allow_all_outbound=True,
            security_group_name=f"airflow-{self.deploy_env.value}-sg",
        )

        self.security_group.add_ingress_rule(
            peer=ec2.Peer.ipv4("0.0.0.0/0"), connection=ec2.Port.tcp(5432)
        )

        self.bucket = s3.Bucket(
            self,
            id=f"s3-{self.deploy_env.value}-belisco-airflow",
            bucket_name=f"s3-{self.deploy_env.value}-belisquinho-airflow",
            removal_policy=core.RemovalPolicy.DESTROY,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL
        )

        self.execution_role = iam.Role(
            self,
            id=f"iam-{self.deploy_env.value}-data-lake-raw-dms-role",
            description="Role to allow Airflow to access resources",
            assumed_by=iam.ServicePrincipal("airflow.amazonaws.com"),
        )
        self.execution_role.assume_role_policy.add_statements(
            iam.PolicyStatement(principals=[iam.ServicePrincipal("airflow-env.amazonaws.com")], actions=["sts:AssumeRole"]))

        self.execution_policy = iam.Policy(
            self,
            id=f"iam-{self.deploy_env.value}-airflow-execution-policy",
            policy_name=f"iam-{self.deploy_env.value}-airflow-execution-policy",
            statements=[
                iam.PolicyStatement(
                    actions=[
                        "s3:PutObjectTagging",
                        "s3:DeleteObject",
                        "s3:ListBucket",
                        "s3:GetObject",
                        "s3:PutObject",
                    ],
                    resources=[
                        self.data_lake_raw_bucket.bucket_arn,
                        f"{self.data_lake_raw_bucket.bucket_arn}/*",
                    ],
                ),
                iam.PolicyStatement(
                    actions=[
                        "airflow:PublishMetrics"
                    ],
                    resources=[
                        f"arn:aws:airflow:{self.region}:{self.account}:environment/{self.deploy_env.value}-airflow"
                    ],
                ),
                iam.PolicyStatement(
                    actions=[
                        "airflow:PublishMetrics"
                    ],
                    resources=[
                        f"arn:aws:airflow:{self.region}:{self.account}:environment/{self.deploy_env.value}-airflow"
                    ],
                ),
                iam.PolicyStatement(
                    actions=[
                        "s3:GetObject*",
                        "s3:GetBucket*",
                        "s3:List*"
                    ],
                    resources=[
                        f"{self.bucket.bucket_arn}/*",
                        f"{self.bucket.bucket_arn}"
                    ]
                ),
                iam.PolicyStatement(
                    actions=[
                        "logs:CreateLogStream",
                        "logs:CreateLogGroup",
                        "logs:PutLogEvents",
                        "logs:GetLogEvents",
                        "logs:GetLogRecord",
                        "logs:GetLogGroupFields",
                        "logs:GetQueryResults"
                    ],
                    resources=[
                        self.log_group.log_group_arn,
                        f"{self.log_group.log_group_arn}*",
                    ]
                ),
                iam.PolicyStatement(
                    actions=[
                        "logs:DescribeLogGroups"
                    ],
                    resources=["*"]
                ),
                iam.PolicyStatement(
                    actions=[
                        "cloudwatch:PutMetricData"
                    ],
                    resources=["*"]
                ),
                iam.PolicyStatement(
                    actions=[
                        "sqs:ChangeMessageVisibility",
                        "sqs:DeleteMessage",
                        "sqs:GetQueueAttributes",
                        "sqs:GetQueueUrl",
                        "sqs:ReceiveMessage",
                        "sqs:SendMessage"
                    ],
                    resources=[
                        f"arn:aws:sqs:{self.region}:*:airflow-celery-*"
                    ]
                )
            ],
        )

        self.execution_role.attach_inline_policy(self.execution_policy)

        self.airflow = mwaa.CfnEnvironment(
            self,
            id=f"{self.deploy_env.value}-airflow",
            name=f"{self.deploy_env.value}-airflow",
            airflow_version="1.10.12",
            dag_s3_path="dags",
            environment_class="mw1.small",
            execution_role_arn=self.execution_role.role_arn,
            logging_configuration=mwaa.CfnEnvironment.LoggingConfigurationProperty(
                dag_processing_logs=self.logging_configuration,
                scheduler_logs=self.logging_configuration,
                task_logs=self.logging_configuration,
                webserver_logs=self.logging_configuration,
                worker_logs=self.logging_configuration
            ),
            max_workers=2,
            min_workers=1,
            network_configuration=mwaa.CfnEnvironment.NetworkConfigurationProperty(
                security_group_ids=[self.security_group.security_group_id],
                subnet_ids=[subnet.subnet_id for subnet in self.common_stack.custom_vpc.private_subnets]
            ),
            webserver_access_mode="PUBLIC_ONLY",
            weekly_maintenance_window_start="WED:01:00",
            source_bucket_arn=self.bucket.bucket_arn
        )

        with ZipFile('data_platform/airflow/resources.zip', 'w') as zipObj2:
            zipObj2.write("data_platform/airflow/requirements.txt", arcname="requirements.txt")
            for file in os.listdir("data_platform/airflow/dags"):
                zipObj2.write(f"data_platform/airflow/dags/{file}", arcname=f"dags/{file}")

        s3deploy.BucketDeployment(
            self,
            id=f"{self.deploy_env.value}-belisquinho-airflow-content",
            destination_bucket=self.bucket,
            sources=[s3deploy.Source.asset(_get_abs_path("data_platform/airflow/resources.zip"))]
        )
