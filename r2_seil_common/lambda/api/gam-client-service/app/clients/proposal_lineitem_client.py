import logging

from aws_xray_sdk.core import xray_recorder
from dependency_injector.providers import Configuration

from app.models.perform_proposal_lineitem_action_request import (
    PerformProposalLineitemActionRequest,
)
from app.models.perform_proposal_lineitem_action_response import (
    PerformProposalLineItemResponse,
)
from gam_core.model.bad_request import BadRequest
from gam_core.model.guard_exception import GuardException
from gam_core.model.success_response import SuccessResponse
from googleads import ad_manager
from googleads.common import ZeepServiceProxy

from app.models.create_proposal_lineitem_request import CreateProposalLineItemRequest
from app.models.create_proposal_lineitem_response import CreateProposalLineItemResponse
from app.models.update_proposal_lineitem_request import UpdateProposalLineItemRequest
from app.models.update_proposal_lineitem_response import UpdateProposalLineItemResponse
from app.models.get_proposal_lineitem_request import GetProposalLineItemRequest
from app.models.get_proposal_lineitem_response import GetProposalLineItemResponse
from gam_core.utils import validate_request


class ProposalLineItemClient:
    supported_actions = {
        "archive": "ArchiveProposalLineItems",
    }

    def __init__(
        self, config: Configuration, ad_manager_client: ad_manager.AdManagerClient
    ):
        # Turn off caching to prevent zeep writing a disk cache in a read-only lambda filesystem
        ad_manager_client.cache = ZeepServiceProxy.NO_CACHE
        self.api_version = config["gam_api_version"]
        self.client = ad_manager_client.GetService(
            "ProposalLineItemService", self.api_version
        )
        self.logger = logging.getLogger(__name__)

    @xray_recorder.capture("ProposalLineItemClient.create_proposal_lineitem")
    def create_proposal_lineitem(self, request: dict) -> SuccessResponse:
        if type(request) is not dict:
            raise GuardException(
                f"Request must be valid JSON and not type {type(request)}"
            )

        headers = request.get("headers", {})
        if len(headers.keys()) < 1:
            raise GuardException("Expected headers are missing")

        body = request.get("body", {})
        if len(body.keys()) < 1:
            raise GuardException("Expected body is missing")

        create_proposal_lineitem = CreateProposalLineItemRequest(body)
        proposal_lineitems = self.client.createProposalLineItems(
            create_proposal_lineitem.get_request()
        )
        response = CreateProposalLineItemResponse(proposal_lineitems).get_response()

        return SuccessResponse(headers, **response)

    @xray_recorder.capture("ProposalLineItemClient.update_proposal_lineitem")
    def update_proposal_lineitem(self, request: dict) -> SuccessResponse:
        if type(request) is not dict:
            raise GuardException(
                f"Request must be valid JSON and not type {type(request)}"
            )

        headers = request.get("headers", {})
        if len(headers.keys()) < 1:
            raise GuardException("Expected headers are missing")

        body = request.get("body", {})
        if len(body.keys()) < 1:
            raise GuardException("Expected body is missing")

        update_proposal_lineitem = UpdateProposalLineItemRequest(self.api_version, body)

        response = self.client.getProposalLineItemsByStatement(
            update_proposal_lineitem.get_proposal_lineitem_statement().ToStatement()
        )
        if "results" not in response and len(response["results"]) < 1:
            raise GuardException("Proposal Line Item not found")

        update_response = self.client.updateProposalLineItems(
            update_proposal_lineitem.update_proposal_lineitem(response["results"])
        )
        proposal_response = UpdateProposalLineItemResponse(
            update_response
        ).get_response()

        return SuccessResponse(headers, **proposal_response)

    @xray_recorder.capture("ProposalLineItemClient.perform_proposal_lineitem_action")
    def perform_proposal_lineitem_action(
        self, action: str, request: dict
    ) -> SuccessResponse:
        validate_request(request, required_body_fields=["proposalLineItemIds"])
        action = action.lower()
        headers: dict = request["headers"]
        body: dict = request["body"]
        if action not in self.supported_actions.keys():
            raise GuardException(f"Unsupported order action: {action}")
        proposal_lineitem_action_reqeust = PerformProposalLineitemActionRequest(
            api_version=self.api_version,
            proposal_lineitem_ids=body["proposalLineItemIds"],
            action=self.supported_actions[action],
        )

        num_changes = 0
        should_continue = True
        while should_continue:
            lineitems = self.client.getProposalLineItemsByStatement(
                proposal_lineitem_action_reqeust.statement.ToStatement()
            )
            if "results" in lineitems and len(lineitems["results"]):
                self.logger.info(
                    f"Performing action '{action}' on proposal line items: "
                    ",".join(str(r["id"]) for r in lineitems["results"])
                )
                response = self.client.performProposalLineItemAction(
                    proposal_lineitem_action_reqeust.proposal_lineitem_action,
                    proposal_lineitem_action_reqeust.statement.ToStatement(),
                )
                if response and int(response["numChanges"]) > 0:
                    num_changes += int(response["numChanges"])
                    proposal_lineitem_action_reqeust.statement.offset += (
                        proposal_lineitem_action_reqeust.statement.limit
                    )
            else:
                should_continue = False

        return SuccessResponse(
            headers, **PerformProposalLineItemResponse(num_changes).get_response()
        )

    @xray_recorder.capture("ProposalLineItemClient.get_proposalLineItem")
    def get_proposal_lineitem(self, request: dict) -> SuccessResponse:
        if type(request) is not dict:
            raise GuardException(
                f"Request must be valid JSON and not type {type(request)}"
            )

        headers = request.get("headers", {})
        if len(headers.keys()) < 1:
            raise GuardException("Expected headers are missing")

        path = request.get("transactionContext", {}).get("path")
        proposal_id = path.split("/")[-1]
        if proposal_id is None:
            return BadRequest(
                "400001", "Missing Proposal ID in path", headers
            ).get_response()

        params = {"id": proposal_id}
        get_proposal_lineitems = GetProposalLineItemRequest(self.api_version, params)

        proposal_lineitems = self.client.getProposalLineItemsByStatement(
            get_proposal_lineitems.get_proposal_lineitem_statement().ToStatement()
        )

        get_proposal_lineitem_response = GetProposalLineItemResponse(
            proposal_lineitems["results"]
        ).get_response()

        return SuccessResponse(headers, **get_proposal_lineitem_response)
