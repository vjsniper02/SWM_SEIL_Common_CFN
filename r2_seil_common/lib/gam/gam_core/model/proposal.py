# Represents a single proposal in Google Ad Manager.
from gam_core.model.guard_exception import GuardException
import json

MANDATORY_KEYS = ["name"]


class Proposal:
    def __init__(self, **kwargs):
        missing_keys = [k for k in MANDATORY_KEYS if k not in kwargs.keys()]
        if len(missing_keys) > 0:
            raise GuardException(
                f"The following required proposal fields are missing: {missing_keys}"
            )
        self.__dict__.update(**kwargs)

    def get_request(self):
        return {
            "name": self.__dict__.get("name", None),
            "startDateTime": self.__dict__.get("startDateTime", None),
            "endDateTime": self.__dict__.get("endDateTime", None),
            "advertiser": self.__dict__.get("advertiser", None),
            "agencies": self.__dict__.get("agencies", None),
            "internalNotes": self.__dict__.get("internalNotes", None),
            "primarySalesperson": self.__dict__.get("primarySalesperson", None),
            "salesPlannerIds": self.__dict__.get("salesPlannerIds", None),
            "primaryTraffickerId": self.__dict__.get("primaryTraffickerId", None),
            "sellerContactIds": self.__dict__.get("sellerContactIds", None),
            "appliedTeamIds": self.__dict__.get("appliedTeamIds", None),
            "appliedLabels": self.__dict__.get("appliedLabels", None),
            "effectiveAppliedLabels": self.__dict__.get("effectiveAppliedLabels", None),
            "currencyCode": self.__dict__.get("currencyCode", None),
            "marketplaceInfo": self.__dict__.get("marketplaceInfo", None),
            "buyerRfp": self.__dict__.get("buyerRfp", None),
            "hasBuyerRfp": self.__dict__.get("hasBuyerRfp", None),
            "deliveryPausingEnabled": self.__dict__.get("deliveryPausingEnabled", None),
        }
