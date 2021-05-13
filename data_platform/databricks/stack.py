from aws_cdk import core
from aws_cdk import (
    aws_iam as iam,
    aws_s3 as s3,
)
from data_platform import active_environment


class DatabricksStack(core.Stack):
    """
    Docs followed to create permissions:
    https://docs.databricks.com/administration-guide/account-settings/aws-accounts.html

    https://docs.databricks.com/administration-guide/cloud-configurations/aws/instance-profiles.html

    https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html

    https://docs.databricks.com/data/metastores/aws-glue-metastore.html
    + spark.databricks.hive.metastore.glueCatalog enabled

    https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-glue-data-catalog-hive.html
    """

    def __init__(self, scope: core.Construct, **kwargs) -> None:
        self.deploy_env = active_environment
        super().__init__(scope, id=f"{self.deploy_env.value}-databricks-stack", **kwargs)

        cross_account_role = iam.Role(
            self,
            id=f"iam-{self.deploy_env.value}-databricks-cross-account-role",
            assumed_by=iam.AccountPrincipal(account_id="414351767826"),
            description=f"Allows databricks access to account",
        )

        cross_account_policy = iam.Policy(
            self,
            id=f"iam-{self.deploy_env.value}-databricks-cross-account-policy",
            policy_name=f"iam-{self.deploy_env.value}-databricks-cross-account-policy",
            statements=[
                iam.PolicyStatement(
                    actions=[
                        "ec2:AssociateDhcpOptions",
                        "ec2:AssociateIamInstanceProfile",
                        "ec2:AssociateRouteTable",
                        "ec2:AttachInternetGateway",
                        "ec2:AttachVolume",
                        "ec2:AuthorizeSecurityGroupEgress",
                        "ec2:AuthorizeSecurityGroupIngress",
                        "ec2:CancelSpotInstanceRequests",
                        "ec2:CreateDhcpOptions",
                        "ec2:CreateInternetGateway",
                        "ec2:CreateKeyPair",
                        "ec2:CreatePlacementGroup",
                        "ec2:CreateRoute",
                        "ec2:CreateSecurityGroup",
                        "ec2:CreateSubnet",
                        "ec2:CreateTags",
                        "ec2:CreateVolume",
                        "ec2:CreateVpc",
                        "ec2:CreateVpcPeeringConnection",
                        "ec2:DeleteInternetGateway",
                        "ec2:DeleteKeyPair",
                        "ec2:DeletePlacementGroup",
                        "ec2:DeleteRoute",
                        "ec2:DeleteRouteTable",
                        "ec2:DeleteSecurityGroup",
                        "ec2:DeleteSubnet",
                        "ec2:DeleteTags",
                        "ec2:DeleteVolume",
                        "ec2:DeleteVpc",
                        "ec2:DescribeAvailabilityZones",
                        "ec2:DescribeIamInstanceProfileAssociations",
                        "ec2:DescribeInstanceStatus",
                        "ec2:DescribeInstances",
                        "ec2:DescribePlacementGroups",
                        "ec2:DescribePrefixLists",
                        "ec2:DescribeReservedInstancesOfferings",
                        "ec2:DescribeRouteTables",
                        "ec2:DescribeSecurityGroups",
                        "ec2:DescribeSpotInstanceRequests",
                        "ec2:DescribeSpotPriceHistory",
                        "ec2:DescribeSubnets",
                        "ec2:DescribeVolumes",
                        "ec2:DescribeVpcs",
                        "ec2:DetachInternetGateway",
                        "ec2:DisassociateIamInstanceProfile",
                        "ec2:ModifyVpcAttribute",
                        "ec2:ReplaceIamInstanceProfileAssociation",
                        "ec2:RequestSpotInstances",
                        "ec2:RevokeSecurityGroupEgress",
                        "ec2:RevokeSecurityGroupIngress",
                        "ec2:RunInstances",
                        "ec2:TerminateInstances",
                    ],
                    resources=["*"],
                ),
                iam.PolicyStatement(
                    actions=["iam:CreateServiceLinkedRole", "iam:PutRolePolicy"],
                    resources=[
                        "arn:aws:iam::*:role/aws-service-role/spot.amazonaws.com/AWSServiceRoleForEC2Spot"
                    ],
                    conditions={
                        "StringEquals": {"iam:AWSServiceName": "spot.amazonaws.com"}
                    },
                ),
                iam.PolicyStatement(
                    actions=[
                        "s3:GetBucketNotification",
                        "s3:PutBucketNotification",
                        "sns:ListSubscriptionsByTopic",
                        "sns:GetTopicAttributes",
                        "sns:SetTopicAttributes",
                        "sns:CreateTopic",
                        "sns:TagResource",
                        "sns:Publish",
                        "sns:Subscribe",
                        "sqs:CreateQueue",
                        "sqs:DeleteMessage",
                        "sqs:DeleteMessageBatch",
                        "sqs:ReceiveMessage",
                        "sqs:SendMessage",
                        "sqs:GetQueueUrl",
                        "sqs:GetQueueAttributes",
                        "sqs:SetQueueAttributes",
                        "sqs:TagQueue",
                        "sqs:ChangeMessageVisibility",
                        "sqs:ChangeMessageVisibilityBatch",
                    ],
                    resources=[
                        f"arn:aws:s3:::s3-belisco-{self.deploy_env.value}-data-lake-*",
                        f"arn:aws:sqs:{self.region}:{self.account}:databricks-auto-ingest-*",
                        f"arn:aws:sns:{self.region}:{self.account}:databricks-auto-ingest-*",
                    ],
                ),
                iam.PolicyStatement(
                    actions=["sqs:ListQueues", "sqs:ListQueueTags", "sns:ListTopics"],
                    resources=["*"],
                ),
                iam.PolicyStatement(
                    actions=["sns:Unsubscribe", "sns:DeleteTopic", "sqs:DeleteQueue"],
                    resources=[
                        f"arn:aws:sqs:{self.region}:{self.account}:databricks-auto-ingest-*",
                        f"arn:aws:sns:{self.region}:{self.account}:databricks-auto-ingest-*",
                    ],
                ),
            ],
        )
        cross_account_role.attach_inline_policy(cross_account_policy)

        bucket = s3.Bucket(
            self,
            id=f"s3-{self.deploy_env.value}-belisco-databricks-bucket",
            bucket_name=f"s3-{self.deploy_env.value}-belisco-databricks-bucket",
        )
        bucket.add_to_resource_policy(
            iam.PolicyStatement(
                principals=[iam.AccountPrincipal(account_id="414351767826")],
                effect=iam.Effect.ALLOW,
                actions=[
                    "s3:GetObject",
                    "s3:GetObjectVersion",
                    "s3:PutObject",
                    "s3:DeleteObject",
                    "s3:ListBucket",
                    "s3:GetBucketLocation",
                ],
                resources=[bucket.bucket_arn, bucket.bucket_arn + "/*"],
            )
        )

        access_role = iam.Role(
            self,
            id=f"iam-{self.deploy_env.value}-databricks-data-lake-access-role",
            assumed_by=iam.ServicePrincipal("ec2"),
            description=f"Allows databricks access to data lake",
        )

        access_policy = iam.Policy(
            self,
            id=f"iam-{self.deploy_env.value}-databricks-data-lake-access-policy",
            policy_name=f"iam-{self.deploy_env.value}-databricks-data-lake-access-policy",
            statements=[
                iam.PolicyStatement(
                    actions=[
                        "s3:ListBucket",
                        "s3:PutObject",
                        "s3:GetObject",
                        "s3:DeleteObject",
                        "s3:PutObjectAcl",
                    ],
                    resources=[
                        f"arn:aws:s3:::s3-belisco-turma-4-{self.deploy_env.value}-data-lake-*",
                        f"arn:aws:s3:::s3-belisco-turma-4-{self.deploy_env.value}-data-lake-*/*",
                    ],
                ),
                iam.PolicyStatement(
                    actions=["iam:CreateServiceLinkedRole", "iam:PutRolePolicy"],
                    resources=[
                        "arn:aws:iam::*:role/aws-service-role/spot.amazonaws.com/AWSServiceRoleForEC2Spot"
                    ],
                    conditions={
                        "StringEquals": {"iam:AWSServiceName": "spot.amazonaws.com"}
                    },
                ),
                iam.PolicyStatement(
                    actions=[
                        "glue:BatchCreatePartition",
                        "glue:BatchDeletePartition",
                        "glue:BatchGetPartition",
                        "glue:CreateDatabase",
                        "glue:CreateTable",
                        "glue:CreateUserDefinedFunction",
                        "glue:DeleteDatabase",
                        "glue:DeletePartition",
                        "glue:DeleteTable",
                        "glue:DeleteUserDefinedFunction",
                        "glue:GetDatabase",
                        "glue:GetDatabases",
                        "glue:GetPartition",
                        "glue:GetPartitions",
                        "glue:GetTable",
                        "glue:GetTables",
                        "glue:GetUserDefinedFunction",
                        "glue:GetUserDefinedFunctions",
                        "glue:UpdateDatabase",
                        "glue:UpdatePartition",
                        "glue:UpdateTable",
                        "glue:UpdateUserDefinedFunction",
                    ],
                    resources=["*"],
                ),
            ],
        )
        access_role.attach_inline_policy(access_policy)

        cross_account_policy_data_access = iam.Policy(
            self,
            id=f"iam-{self.deploy_env.value}-databricks-cross-account-policy-data-access",
            policy_name=f"iam-{self.deploy_env.value}-databricks-cross-account-policy-data-access",
            statements=[
                iam.PolicyStatement(
                    actions=[
                        "iam:PassRole",
                    ],
                    resources=[access_role.role_arn],
                )
            ],
        )
        iam.CfnInstanceProfile(
            self,
            id=f"iam-{self.deploy_env.value}-databricks-data-lake-access-instance-profile",
            instance_profile_name=f"iam-{self.deploy_env.value}-databricks-data-lake-access-instance-profile",
            roles=[access_role.role_name],
        )

        cross_account_role.attach_inline_policy(cross_account_policy_data_access)
