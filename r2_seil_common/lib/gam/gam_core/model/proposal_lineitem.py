# Represents a single proposal in Google Ad Manager.
from gam_core.model.guard_exception import GuardException
import json

MANDATORY_KEYS = [
    "proposalId",
    "name",
    "deliveryRateType",
    "startDateTime",
    "endDateTime",
    "netRate",
    "targeting",
    "creativePlaceholders",
    "rateType",
    "lineItemType",
]


class ProposalLineItem:
    def __init__(self, **kwargs):
        missing_keys = [k for k in MANDATORY_KEYS if k not in kwargs.keys()]
        if len(missing_keys) > 0:
            raise GuardException(
                f"The following required proposal line item fields are missing: {missing_keys}"
            )
        self.__dict__.update(**kwargs)

    def get_request(self):
        return {
            "id": self.__dict__.get("id", None),
            "proposalId": self.__dict__.get("proposalId", None),
            "name": self.__dict__.get("name", None),
            "startDateTime": self.__dict__.get("startDateTime", None),
            "endDateTime": self.__dict__.get("endDateTime", None),
            "timeZoneId": self.__dict__.get("timeZoneId", None),
            "internalNotes": self.__dict__.get("internalNotes", None),
            "isArchived": self.__dict__.get("isArchived", None),
            "goal": self.__dict__.get("goal", None),
            "contractedUnitsBought": self.__dict__.get("contractedUnitsBought", None),
            "deliveryRateType": self.__dict__.get("deliveryRateType", None),
            "roadblockingType": self.__dict__.get("roadblockingType", None),
            "companionDeliveryOption": self.__dict__.get(
                "companionDeliveryOption", None
            ),
            "videoMaxDuration": self.__dict__.get("videoMaxDuration", None),
            "videoCreativeSkippableAdType": self.__dict__.get(
                "videoCreativeSkippableAdType", None
            ),
            "frequencyCaps": self.__dict__.get("frequencyCaps", None),
            "dfpLineItemId": self.__dict__.get("dfpLineItemId", None),
            "lineItemType": self.__dict__.get("lineItemType", None),
            "lineItemPriority": self.__dict__.get("lineItemPriority", None),
            "rateType": self.__dict__.get("rateType", None),
            "creativePlaceholders": self.__dict__.get("creativePlaceholders", None),
            "targeting": self.__dict__.get("targeting", None),
            "customFieldValues": self.__dict__.get("customFieldValues", None),
            "appliedLabels": self.__dict__.get("appliedLabels", None),
            "effectiveAppliedLabels": self.__dict__.get("effectiveAppliedLabels", None),
            "disableSameAdvertiserCompetitiveExclusion": self.__dict__.get(
                "disableSameAdvertiserCompetitiveExclusion", None
            ),
            "isSold": self.__dict__.get("isSold", None),
            "netRate": self.__dict__.get("netRate", None),
            "netCost": self.__dict__.get("netCost", None),
            "deliveryIndicator": self.__dict__.get("deliveryIndicator", None),
            "deliveryData": self.__dict__.get("deliveryData", None),
            "computedStatus": self.__dict__.get("computedStatus", None),
            "lastModifiedDateTime": self.__dict__.get("lastModifiedDateTime", None),
            "reservationStatus": self.__dict__.get("reservationStatus", None),
            "lastReservationDateTime": self.__dict__.get(
                "lastReservationDateTime", None
            ),
            "environmentType": self.__dict__.get("environmentType", None),
            "allowedFormats": self.__dict__.get("allowedFormats", None),
            "isProgrammatic": self.__dict__.get("isProgrammatic", None),
            "additionalTerms": self.__dict__.get("additionalTerms", None),
            "programmaticCreativeSource": self.__dict__.get(
                "programmaticCreativeSource", None
            ),
            "grpSettings": self.__dict__.get("grpSettings", None),
            "estimatedMinimumImpressions": self.__dict__.get(
                "estimatedMinimumImpressions", None
            ),
            "thirdPartyMeasurementSettings": self.__dict__.get(
                "thirdPartyMeasurementSettings", None
            ),
            "makegoodInfo": self.__dict__.get("makegoodInfo", None),
            "hasMakegood": self.__dict__.get("hasMakegood", None),
            "canCreateMakegood": self.__dict__.get("canCreateMakegood", None),
            "pauseRole": self.__dict__.get("pauseRole", None),
            "pauseReason": self.__dict__.get("pauseReason", None),
            "internalNotes": self.__dict__.get("internalNotes", None),
        }
