from aws_cdk import core
from aws_cdk import (
    aws_glue as glue,
    aws_iam as iam,
)
from data_platform.data_lake.base import BaseDataLakeBucket


class BaseDataLakeGlueDatabase(glue.Database):
    """
    Creates a glue database associated to a data lake bucket
    """

    def __init__(
        self, scope: core.Construct, data_lake_bucket: BaseDataLakeBucket, **kwargs
    ) -> None:
        self.data_lake_bucket = data_lake_bucket
        self.deploy_env = scope.deploy_env
        self.obj_name = f"glue-belisco-{self.deploy_env.value}-data-lake-{self.data_lake_bucket.layer.value}"

        super().__init__(
            scope,
            self.obj_name,
            database_name=self.database_name,
            location_uri=self.location_uri,
        )

    @property
    def database_name(self):
        """
        Returns the glue database name
        """
        return self.obj_name.replace("-", "_")

    @property
    def location_uri(self):
        """
        Returns the database location
        """
        return f"s3://{self.data_lake_bucket.bucket_name}"


class BaseDataLakeGlueRole(iam.Role):
    def __init__(
        self, scope: core.Construct, data_lake_bucket: BaseDataLakeBucket, **kwargs
    ) -> None:
        self.data_lake_bucket = data_lake_bucket
        self.deploy_env = scope.deploy_env
        self.layer = self.data_lake_bucket.layer
        super().__init__(
            scope,
            id=f"iam-{self.deploy_env.value}-glue-data-lake-{self.layer.value}-role",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            description=f"Allows using Glue on Data Lake {self.layer.value}",
        )
        self.bucket_arn = self.data_lake_bucket.bucket_arn
        self.add_policy()
        self.add_instance_profile()

    def add_policy(self):
        policy = iam.Policy(
            self,
            id=f"iam-{self.deploy_env.value}-glue-data-lake-{self.layer.value}-policy",
            policy_name=f"iam-{self.deploy_env.value}-glue-data-lake-{self.layer.value}-policy",
            statements=[
                iam.PolicyStatement(
                    actions=["s3:ListBucket", "s3:GetObject", "s3:PutObject"],
                    resources=[self.bucket_arn, f"{self.bucket_arn}/*"],
                ),
                iam.PolicyStatement(
                    actions=["cloudwatch:PutMetricData"],
                    resources=["arn:aws:cloudwatch:*"],
                ),
                iam.PolicyStatement(actions=["glue:*"], resources=["arn:aws:glue:*"]),
                iam.PolicyStatement(
                    actions=[
                        "logs:CreateLogGroup",
                        "logs:CreateLogStream",
                        "logs:PutLogEvents",
                    ],
                    resources=["arn:aws:logs:*:*:/aws-glue/*"],
                ),
            ],
        )
        self.attach_inline_policy(policy)

    def add_instance_profile(self):
        iam.CfnInstanceProfile(
            self,
            id=f"iam-{self.deploy_env.value}-glue-data-lake-{self.layer.value}-instance-profile",
            instance_profile_name=f"iam-{self.deploy_env.value}-glue-data-lake-{self.layer.value}-instance-profile",
            roles=[self.role_name],
        )


class BaseGlueCrawler(glue.CfnCrawler):
    def __init__(
        self,
        scope: core.Construct,
        table_name: str,
        glue_database: BaseDataLakeGlueDatabase,
        schedule_expression: str,
        glue_role: BaseDataLakeGlueRole,
        **kwargs,
    ) -> None:

        self.glue_database = glue_database
        self.glue_role = glue_role
        self.schedule_expression = schedule_expression
        self.table_name = table_name
        self.deploy_env = self.glue_database.deploy_env
        self.data_lake_bucket = self.glue_database.data_lake_bucket
        self.obj_name = f"glue-{self.deploy_env.value}-{self.data_lake_bucket.layer.value}-{self.table_name}-crawler"
        super().__init__(
            scope,
            id=self.obj_name,
            name=self.obj_name,
            description=f"Crawler para detectar o schema de dados armazenados em "
            f"Data Lake {self.data_lake_bucket.layer.value}.{self.table_name}",
            schedule=self.crawler_schedule,
            role=self.glue_role.role_arn,
            database_name=self.glue_database.database_name,
            targets=self.targets,
            **kwargs,
        )

    @property
    def crawler_schedule(self):
        return glue.CfnCrawler.ScheduleProperty(
            schedule_expression=self.schedule_expression
        )

    @property
    def targets(self):
        return glue.CfnCrawler.TargetsProperty(
            s3_targets=[
                glue.CfnCrawler.S3TargetProperty(
                    path=f"s3://{self.data_lake_bucket.bucket_name}/{self.table_name}"
                ),
             ]
        )


class OrdersTable(glue.Table):
    def __init__(
        self,
        scope: core.Construct,
        glue_database: BaseDataLakeGlueDatabase,
        glue_role: BaseDataLakeGlueRole,
        **kwargs,
    ) -> None:
        self.glue_role = glue_role
        self.glue_database = glue_database
        self.deploy_env = self.glue_database.deploy_env
        self.data_lake_bucket = self.glue_database.data_lake_bucket
        self.obj_name = f"glue-{self.deploy_env.value}-orders-table"
        super().__init__(
            scope,
            self.obj_name,
            table_name="orders",
            description="orders captured from Postgres using DMS CDC",
            database=self.glue_database,
            compressed=True,
            data_format=glue.DataFormat.PARQUET,
            s3_prefix="orders/public/orders",
            bucket=self.data_lake_bucket,
            columns=[
                glue.Column(
                    name="op", type=glue.Type(input_string="string", is_primitive=True)
                ),
                glue.Column(
                    name="extracted_at",
                    type=glue.Type(input_string="string", is_primitive=True),
                ),
                glue.Column(
                    name="created_at",
                    type=glue.Type(input_string="timestamp", is_primitive=True),
                ),
                glue.Column(
                    name="order_id", type=glue.Type(input_string="int", is_primitive=True)
                ),
                glue.Column(
                    name="product_name",
                    type=glue.Type(input_string="string", is_primitive=True),
                ),
                glue.Column(
                    name="value", type=glue.Type(input_string="double", is_primitive=True)
                ),
            ],
            **kwargs,
        )


class OrdersV2Table(glue.Table):
    def __init__(
        self,
        scope: core.Construct,
        glue_database: BaseDataLakeGlueDatabase,
        glue_role: BaseDataLakeGlueRole,
        **kwargs,
    ) -> None:
        self.glue_role = glue_role
        self.glue_database = glue_database
        self.deploy_env = self.glue_database.deploy_env
        self.data_lake_bucket = self.glue_database.data_lake_bucket
        self.obj_name = f"glue-{self.deploy_env.value}-orders-table"
        super().__init__(
            scope,
            self.obj_name,
            table_name="orders",
            description="orders captured from Postgres using DMS CDC",
            database=self.glue_database,
            compressed=True,
            data_format=glue.DataFormat.PARQUET,
            s3_prefix="orders/public/orders",
            bucket=self.data_lake_bucket,
            columns=[
                glue.Column(
                    name="op", type=glue.Type(input_string="string", is_primitive=True)
                ),
                glue.Column(
                    name="extracted_at",
                    type=glue.Type(input_string="string", is_primitive=True),
                ),
                glue.Column(
                    name="created_at",
                    type=glue.Type(input_string="timestamp", is_primitive=True),
                ),
                glue.Column(
                    name="order_id", type=glue.Type(input_string="string", is_primitive=True)
                ),
                glue.Column(
                    name="product_name",
                    type=glue.Type(input_string="string", is_primitive=True),
                ),
                glue.Column(
                    name="value", type=glue.Type(input_string="double", is_primitive=True)
                ),
            ],
            **kwargs,
        )