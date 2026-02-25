import sys
from app.container import bootstrap
from app.lambda_handler import handler
from app.techone_client import TechOneClient
from app.techone_client_mock import TechOneClientMock

if not hasattr(sys, "_called_from_test"):
    bootstrap(TechOneClient, TechOneClientMock, modules=[handler.__module__])
