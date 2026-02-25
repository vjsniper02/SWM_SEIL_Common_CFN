import sys

from salesforce_core.container import bootstrap as sf_bootstrap
from tech1_ci.container import bootstrap as t1_bootstrap
from .lambda_handler import handler

if not hasattr(sys, "_called_from_test"):
    sf_bootstrap(modules=[handler.__module__])
    t1_bootstrap(modules=[handler.__module__])
