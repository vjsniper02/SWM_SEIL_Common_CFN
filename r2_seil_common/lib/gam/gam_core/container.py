from typing import Optional, Iterable, Any
import os
from boto3 import client as aws_client_factory
from boto3 import resource as aws_resource_factory
from dependency_injector import providers
from dependency_injector.containers import DeclarativeContainer
from googleads import AdManagerClient
from googleads.oauth2 import GoogleServiceAccountClient

from gam_core.client_factory import ClientFactory
from gam_core.service_account import GoogleServiceAccountClientFactory


class Container(DeclarativeContainer):
    config = providers.Configuration()
    aws_secret_client = providers.Callable(
        aws_client_factory,
        service_name="secretsmanager",
        region_name=config.aws_region_name,
    )
    aws_ssm_client = providers.Callable(
        aws_client_factory, service_name="ssm", region_name=config.aws_region_name
    )
    aws_dynamodb_client = providers.Callable(
        aws_client_factory,
        service_name="dynamodb",
        region_name=config.aws_region_name,
    )
    aws_resource_dynamodb = providers.Callable(
        aws_resource_factory,
        "dynamodb",
        region_name=config.aws_region_name,
    )
    gam_oauth2_client = providers.Callable(
        GoogleServiceAccountClientFactory.create_client,
        config=config,
        aws_secret_client=aws_secret_client,
        account_client_factory=GoogleServiceAccountClient,
    )
    gam_client = providers.Factory(
        AdManagerClient,
        oauth2_client=gam_oauth2_client,
        application_name=config.application_name,
        network_code=config.gam_network_code,
    )
    lineitem_api_client = providers.Callable(
        ClientFactory.create_client,
        config=config,
        aws_ssm_client=aws_ssm_client,
        gam_client=gam_client,
        api_client=config.lineitem_api_client,
        api_mock_client=config.lineitem_api_mock_client,
    )
    forecast_api_client = providers.Callable(
        ClientFactory.create_client,
        config=config,
        aws_ssm_client=aws_ssm_client,
        gam_client=gam_client,
        api_client=config.forecast_api_client,
        api_mock_client=config.forecast_api_mock_client,
    )
    order_api_client = providers.Callable(
        ClientFactory.create_client,
        config=config,
        aws_ssm_client=aws_ssm_client,
        gam_client=gam_client,
        api_client=config.order_api_client,
        api_mock_client=config.order_api_mock_client,
    )
    proposal_api_client = providers.Callable(
        ClientFactory.create_client,
        config=config,
        aws_ssm_client=aws_ssm_client,
        gam_client=gam_client,
        api_client=config.proposal_api_client,
        api_mock_client=config.proposal_api_mock_client,
    )
    proposal_lineitem_api_client = providers.Callable(
        ClientFactory.create_client,
        config=config,
        aws_ssm_client=aws_ssm_client,
        gam_client=gam_client,
        api_client=config.proposal_lineitem_api_client,
        api_mock_client=config.proposal_lineitem_api_mock_client,
    )
    proposal_api_client = providers.Callable(
        ClientFactory.create_client,
        config=config,
        aws_ssm_client=aws_ssm_client,
        gam_client=gam_client,
        api_client=config.proposal_api_client,
        api_mock_client=config.proposal_api_mock_client,
    )
    report_api_client = providers.Callable(
        ClientFactory.create_client,
        config=config,
        aws_ssm_client=aws_ssm_client,
        gam_client=gam_client,
        api_client=config.report_api_client,
        api_mock_client=config.report_api_mock_client,
    )


def bootstrap(
    lineitem_api_client: callable(object),
    lineitem_api_mock_client: callable(object),
    order_api_client: callable(object),
    order_api_mock_client: callable(object),
    forecast_api_client: callable(object),
    forecast_api_mock_client: callable(object),
    proposal_api_client: callable(object),
    proposal_api_mock_client: callable(object),
    proposal_lineitem_api_client: callable(object),
    proposal_lineitem_api_mock_client: callable(object),
    report_api_client: callable(object),
    report_api_mock_client: callable(object),
    modules: Optional[Iterable[Any]] = None,
) -> DeclarativeContainer:
    container = Container()

    container.config.lineitem_api_client.from_value(lineitem_api_client)
    container.config.lineitem_api_mock_client.from_value(lineitem_api_mock_client)
    container.config.order_api_client.from_value(order_api_client)
    container.config.order_api_mock_client.from_value(order_api_mock_client)
    container.config.forecast_api_client.from_value(forecast_api_client)
    container.config.forecast_api_mock_client.from_value(forecast_api_mock_client)
    container.config.proposal_api_client.from_value(proposal_api_client)
    container.config.proposal_api_mock_client.from_value(proposal_api_mock_client)
    container.config.proposal_lineitem_api_client.from_value(
        proposal_lineitem_api_client
    )
    container.config.proposal_lineitem_api_mock_client.from_value(
        proposal_lineitem_api_mock_client
    )
    container.config.report_api_client.from_value(report_api_client)
    container.config.report_api_mock_client.from_value(report_api_mock_client)

    container.config.aws_region_name.from_env("AWS_REGION", default="ap-southeast-2")
    container.config.application_name.from_env("GAM_APPLICATION_NAME", default="N/A")
    container.config.environment.from_env("ENVIRONMENT", default="local")
    container.config.gam_account_aws_secret_name.from_env(
        "GAM_ACCOUNT_AWS_SECRET_NAME", default="apidev/gam/service_account_key"
    )
    container.config.gam_mock_aws_param_name.from_env(
        "GAM_MOCK_AWS_PARAM_NAME", default="/apidev/gam/use-mock-gam-client"
    )
    container.config.gam_network_code.from_env(
        "GAM_NETWORK_CODE", default="60035833"
    )  # Seven West Media - DFP APAC
    # overriding os GAM_API_VERSION variable to avoid container variable VALUE being refered if in case its depreciated.
    os.environ["GAM_API_VERSION"] = "v202302"
    container.config.gam_api_version.from_env("GAM_API_VERSION", default="v202302")

    container.wire(modules=modules)

    return container
