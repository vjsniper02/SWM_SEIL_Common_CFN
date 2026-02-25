# Represents a single line item in Google Ad Manager.
# This class allows us to target the client SOAP message with only the fields coded
# in get_request() below, regardless of what comes through in the request object.
from gam_core.model.guard_exception import GuardException

MANDATORY_KEYS = [
    "name",
    "startDateTime",
    "lineItemType",
    "costPerUnit",
    "costType",
    "creativePlaceholders",
    "targeting",
]


class LineItem:
    def __init__(self, **kwargs):
        missing_keys = [k for k in MANDATORY_KEYS if k not in kwargs.keys()]
        if len(missing_keys) > 0:
            raise GuardException(
                f"The following required lineItem fields are missing: {missing_keys}"
            )
        self.__dict__.update(**kwargs)

    def get_request(self):
        return {
            "id": self.__dict__.get("id", None),
            "externalId": self.__dict__.get("externalId", None),
            "orderId": self.__dict__.get("orderId", ""),
            "priority": self.__dict__.get("priority", None),
            "name": self.__dict__.get("name", ""),
            "startDateTime": self.__dict__.get("startDateTime", {}),
            "endDateTime": self.__dict__.get("endDateTime", {}),
            "unlimitedEndDateTime": self.__dict__.get("unlimitedEndDateTime", "false"),
            "lineItemType": self.__dict__.get("lineItemType", "UNKNOWN"),
            "costPerUnit": self.__dict__.get("costPerUnit", {}),
            "costType": self.__dict__.get("costType", "UNKNOWN"),
            "creativePlaceholders": self.__dict__.get("creativePlaceholders", []),
            "primaryGoal": self.__dict__.get("primaryGoal", {}),
            "secondaryGoals": self.__dict__.get("secondaryGoals", None),
            "targeting": self.__dict__.get("targeting", {}),
            "isArchived": self.__dict__.get("isArchived", False),
            "environmentType": self.__dict__.get("environmentType", None),
            "videoMaxDuration": self.__dict__.get("videoMaxDuration", None),
            "roadblockingType": self.__dict__.get("roadblockingType", None),
            "companionDeliveryOption": self.__dict__.get("companionDeliveryOption", None),
            "notes": self.__dict__.get("notes", None),
        }
