# Represents a single line item in Google Ad Manager.
# This class allows us to target the client SOAP message with only the fields coded
# in get_request() below, regardless of what comes through in the request object.
from gam_core.model.guard_exception import GuardException

MANDATORY_KEYS = ["id", "orderId"]
# Comprehensive list from https://developers.google.com/ad-manager/api/reference/v202111/LineItemService.LineItem
ALLOWED_KEYS = [
    "name",
    "creativeRotationType",
    "lineItemType",
    "externalId",
    "orderName",
    "startDateTime",
    "startDateTimeType",
    "endDateTime",
    "autoExtensionDays",
    "unlimitedEndDateTime",
    "deliveryRateType",
    "deliveryForecastSource",
    "customPacingCurve",
    "roadblockingType",
    "skippableAdType",
    "frequencyCaps",
    "priority",
    "costPerUnit",
    "valueCostPerUnit",
    "costType",
    "discountType",
    "discount",
    "contractedUnitsBought",
    "creativePlaceholders",
    "activityAssociations",
    "environmentType",
    "allowedFormats",
    "companionDeliveryOption",
    "allowOverbook",
    "skipInventoryCheck",
    "skipCrossSellingRuleWarningChecks",
    "reserveAtCreation",
    "stats",
    "deliveryIndicator",
    "deliveryData",
    "budget",
    "status",
    "reservationStatus",
    "isArchived",
    "webPropertyCode",
    "appliedLabels",
    "effectiveAppliedLabels",
    "disableSameAdvertiserCompetitiveExclusion",
    "lastModifiedByApp",
    "notes",
    "competitiveConstraintScope",
    "lastModifiedDateTime",
    "creationDateTime",
    "customFieldValues",
    "isMissingCreatives",
    "programmaticCreativeSource",
    "thirdPartyMeasurementSettings",
    "videoMaxDuration",
    "primaryGoal",
    "secondaryGoals",
    "grpSettings",
    "dealInfo",
    "viewabilityProviderCompanyIds",
    "childContentEligibility",
    "customVastExtension",
    "targeting",
    "creativeTargetings",
]


class LineItemUpdate:
    def __init__(self, **kwargs):
        missing_keys = [k for k in MANDATORY_KEYS if k not in kwargs.keys()]
        if len(missing_keys) > 0:
            raise GuardException(
                f"The following required lineItem fields are missing: {missing_keys}"
            )
        self.__dict__.update(**kwargs)
        self.__delta__ = {}
        # Add common keys from the incoming request and allowed+mandatory keys, add to the delta
        for key in set(self.__dict__.keys()).intersection(
            MANDATORY_KEYS + ALLOWED_KEYS
        ):
            self.__delta__[key] = self.__dict__[key]

    def get_request(self):
        return self.__delta__
