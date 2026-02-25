from boto3 import client as aws_client_factory
from dependency_injector import providers
from dependency_injector.containers import DeclarativeContainer
from typing import Optional, Iterable, Any

from tech1_ci.client_factory import ClientFactory


class Container(DeclarativeContainer):
    config = providers.Configuration()
    aws_secret_client = providers.Callable(
        aws_client_factory,
        service_name="secretsmanager",
        region_name=config.aws_region_name,
    )
    do_import = providers.Callable(
        ClientFactory.create_do_import,
        config=config,
        aws_secret_client=aws_secret_client,
    )


def bootstrap(
    modules: Optional[Iterable[Any]] = None,
) -> DeclarativeContainer:
    container = Container()

    container.config.aws_region_name.from_env("AWS_REGION", default="ap-southeast-2")
    container.config.environment.from_env("ENVIRONMENT", default="apidev")
    container.config.t1_warehouse_name.from_env("T1_WAREHOUSE", default="SALESFORCE")
    container.config.t1_warehouse_table_name.from_env(
        "T1_WAREHOUSE_TABLE", default="ARDATA"
    )

    environment = container.config.get("environment")
    container.config.t1_creds_secret.from_env(
        "T1_CREDS_SECRET", f"{environment}/tech1/soap"
    )

    container.wire(modules=modules)
    return container
