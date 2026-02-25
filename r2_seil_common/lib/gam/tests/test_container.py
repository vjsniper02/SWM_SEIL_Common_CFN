from unittest.mock import Mock

import pytest
from dependency_injector import providers
from dependency_injector.wiring import Provide, inject

from gam_core.container import bootstrap, Container


@pytest.fixture(scope="function")
def arrange():
    container = bootstrap(
        Mock(),
        Mock(),
        Mock(),
        Mock(),
        Mock(),
        Mock(),
        Mock(),
        Mock(),
        Mock(),
        Mock(),
        Mock(),
        Mock(),
        [__name__],
    )

    yield container


@pytest.mark.parametrize(
    "dependency_name,provider_type",
    [
        ("gam_client", providers.Factory),
        ("aws_secret_client", providers.Callable),
        ("aws_ssm_client", providers.Callable),
        ("lineitem_api_client", providers.Callable),
        ("forecast_api_client", providers.Callable),
        ("order_api_client", providers.Callable),
        ("gam_oauth2_client", providers.Callable),
    ],
)
def test_container_has_correct_dependencies(dependency_name, provider_type, arrange):
    (container) = arrange

    # Assert
    assert type(container.__dict__[dependency_name]) is provider_type


@inject
@pytest.mark.parametrize(
    "config_name, expected_default_value",
    [
        ("application_name", "N/A"),
        ("aws_region_name", "ap-southeast-2"),
        ("environment", "local"),
        ("gam_account_aws_secret_name", "apidev/gam/service_account_key"),
        ("gam_api_version", "v202302"),
        ("gam_mock_aws_param_name", "/apidev/gam/use-mock-gam-client"),
        ("gam_network_code", "60035833"),
    ],
)
def test_container_has_correct_config(
    config_name, expected_default_value, arrange, config=Provide[Container.config]
):
    (container) = arrange

    # Assert
    assert config_name in config
    assert config[config_name] == expected_default_value
