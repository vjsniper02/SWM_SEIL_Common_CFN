import os
from core.exceptions import GenericException


# This function extract environments variable set on lambda
def extract_environment_variable(variable: str, default=None) -> str:
    try:
        s = os.getenv(variable, default)
        return s
    except KeyError as ke:
        raise GenericException(f"{variable} environment variable could not be found")


# This function captures and reads the payload data for the lambda
def extract_validate_value(data: dict, key: str, nullable: bool):
    try:
        value = data[key]
        if nullable is False:
            if (value == None) or (str(value).strip() == ""):
                raise GenericException("%s cannot be null or empty" % key)
        if str(value).strip() == "":
            return None
        return value
    except KeyError:
        if nullable is True:
            return None
        else:
            raise GenericException("%s value was not specified" % key)
