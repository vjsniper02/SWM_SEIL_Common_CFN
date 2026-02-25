import json
import boto3
import os
from typing import Optional, Iterable, Any
from dependency_injector import providers, containers
from auth_client.client_credentials import ClientAuthToken

from app.techone_client import TechOneClient
from app.techone_client_mock import TechOneClientMock


class Container(containers.DeclarativeContainer):
    config = providers.Configuration()

    auth_client = providers.Singleton(
        ClientAuthToken,
        token_url=config.client_credentials_token_url,
        client_id=config.client_credentials_client_id,
        client_secret=config.client_credentials_client_secret,
    )
    tech1_client = providers.Singleton(
        TechOneClient, config=config, tech1_auth_client=auth_client
    )


def bootstrap(
    techone_client: TechOneClient,
    techone_client_mock: TechOneClientMock,
    modules: Optional[Iterable[Any]] = None,
) -> containers.DeclarativeContainer:
    container = Container()

    container.config.aws_region_name.from_env("AWS_REGION", default="ap-southeast-2")
    container.config.environment.from_env("ENVIRONMENT", default="apidev")
    container.config.tech1_api_scheme.from_env("T1_API_SCHEME", default="https")
    if container.config.get("environment") == "prod":
        os.environ["T1_API_HOST"] = "seven.t1cloud.com"
    container.config.tech1_api_host.from_env(
        "T1_API_HOST", default="seven-test.t1cloud.com"
    )
    container.config.tech1_api_fx_path.from_env(
        "T1_API_FX_PATH",
        default="/T1Default/CiAnywhere/Web/SEVEN-TEST/Api/RaaS/v1/RetrieveUSDFXRATE",
    )
    container.config.tech1_api_debtor_creditlimit_path.from_env(
        "T1_API_DEBTOR_CREDITLIMIT_PATH",
        default="/T1Default/CiAnywhere/Web/SEVEN-TEST/Api/WS/v1/Debtor/RemainingCredit",
    )
    container.config.tech1_api_timeout.from_env("T1_API_TIMEOUT", default=20)

    environment = container.config.get("environment")
    secrets = boto3.client(
        "secretsmanager", region_name=container.config.get("aws_region_name")
    )
    secret_val = secrets.get_secret_value(
        SecretId=f"{environment}/tech1/client_credentials"
    )
    client_credentials = json.loads(secret_val["SecretString"])
    container.config.client_credentials_token_url.from_value(
        client_credentials["token_url"]
    )
    container.config.client_credentials_client_id.from_value(
        client_credentials["client_id"]
    )
    container.config.client_credentials_client_secret.from_value(
        client_credentials["client_secret"]
    )

    container.wire(modules=modules)

    return container
