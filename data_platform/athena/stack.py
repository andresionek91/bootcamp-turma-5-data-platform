from aws_cdk import core

from data_platform.athena.base import BaseAthenaBucket, BaseAthenaWorkgroup
from data_platform.active_environment import active_environment


class AthenaStack(core.Stack):
    def __init__(self, scope: core.Construct, **kwargs) -> None:
        self.deploy_env = active_environment
        super().__init__(scope, id=f'{self.deploy_env.value}-athena', **kwargs)

        self.athena_bucket = BaseAthenaBucket(
            self,
            deploy_env=self.deploy_env
        )

        self.athena_workgroup = BaseAthenaWorkgroup(
            self,
            deploy_env=self.deploy_env,
            athena_bucket=self.athena_bucket,
            gb_scanned_cutoff_per_query=1
        )
