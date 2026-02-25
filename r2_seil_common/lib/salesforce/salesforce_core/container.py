from typing import Optional, Iterable, Any

from boto3 import client as aws_client_factory
from dependency_injector import providers
from dependency_injector.containers import DeclarativeContainer

from salesforce_core.sf_service_account import SalesForceBulkClientFactory
from salesforce_core.sf_service_account import SalesForceClientFactory


class SfContainer(DeclarativeContainer):
    config = providers.Configuration()
    aws_secret_client = providers.Callable(
        aws_client_factory,
        service_name="secretsmanager",
        region_name=config.aws_region_name,
    )
    sf_bulk = providers.Callable(
        SalesForceBulkClientFactory.create_bulk,
        config=config,
        aws_secret_client=aws_secret_client,
    )
    sf = providers.Callable(
        SalesForceClientFactory.create,
        config=config,
        aws_secret_client=aws_secret_client,
    )


def bootstrap(
    modules: Optional[Iterable[Any]] = None,
) -> DeclarativeContainer:
    container = SfContainer()

    container.config.aws_region_name.from_env("AWS_REGION", default="ap-southeast-2")
    container.config.environment.from_env("ENVIRONMENT", default="local")
    environment = container.config.get("environment")

    container.config.sf_account_aws_secret_name.from_env(
        "SF_ACCOUNT_AWS_SECRET_NAME", default=f"{environment}/sf/jwt"
    )
    container.wire(modules=modules)

    return container
