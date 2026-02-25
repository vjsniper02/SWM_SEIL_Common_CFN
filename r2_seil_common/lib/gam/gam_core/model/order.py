# Represents a single order in Google Ad Manager.
# This class allows us to target the client SOAP message with only the fields coded
# in get_request() below, regardless of what comes through in the request object.
from gam_core.model.guard_exception import GuardException

MANDATORY_KEYS = ["name", "advertiserId", "traffickerId"]


class Order:
    def __init__(self, **kwargs):
        missing_keys = [k for k in MANDATORY_KEYS if k not in kwargs.keys()]
        if len(missing_keys) > 0:
            raise GuardException(
                f"The following required orders fields are missing: {missing_keys}"
            )
        self.__dict__.update(**kwargs)

    def get_request(self):
        return {
            "name": self.__dict__.get("name", ""),
            "startDateTime": self.__dict__.get("startDateTime", ""),
            "notes": self.__dict__.get("notes", ""),
            "externalOrderId": self.__dict__.get("externalOrderId", ""),
            "poNumber": self.__dict__.get("poNumber", ""),
            "currencyCode": self.__dict__.get("currencyCode", ""),
            "advertiserId": self.__dict__.get("advertiserId", ""),
            "traffickerId": self.__dict__.get("traffickerId", ""),
            "totalBudget": self.__dict__.get("totalBudget", {}),
            "salespersonId": self.__dict__.get("salespersonId", None),
        }
