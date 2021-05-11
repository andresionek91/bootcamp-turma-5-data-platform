import os
from enum import Enum


class Environment(Enum):
    PRODUCTION = 'production'
    STAGING = 'staging'
    DEVELOP = 'develop'


active_environment = Environment[os.environ['ENVIRONMENT']]