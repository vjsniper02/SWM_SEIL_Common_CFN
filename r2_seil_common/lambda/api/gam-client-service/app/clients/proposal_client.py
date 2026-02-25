import json
import logging
import re

from aws_xray_sdk.core import xray_recorder
from dependency_injector.providers import Configuration
from googleads import ad_manager
from googleads.common import ZeepServiceProxy

from app.models.create_proposal_request import CreateProposalRequest
from app.models.create_proposal_response import CreateProposalResponse
from app.models.get_proposals_request import GetProposalsRequest
from app.models.get_proposals_response import GetProposalsResponse
from app.models.perform_proposal_action_request import PerformProposalActionRequest
from app.models.perform_proposal_action_response import PerformProposalActionResponse
from app.models.update_proposal_request import UpdateProposalRequest
from app.models.update_proposal_response import UpdateProposalResponse
from gam_core.model.bad_request import BadRequest
from gam_core.model.guard_exception import GuardException
from gam_core.model.success_response import SuccessResponse

SUPPORTED_ACTIONS = {
    "REQUEST_BUYER_ACCEPTANCE": "RequestBuyerAcceptance",
    "EDIT_PROPOSALS_FOR_NEGOTIATION": "EditProposalsForNegotiation",
}


class ProposalClient:
    def __init__(
        self, config: Configuration, ad_manager_client: ad_manager.AdManagerClient
    ):
        # Turn off caching to prevent zeep writing a disk cache in a read-only lambda filesystem
        ad_manager_client.cache = ZeepServiceProxy.NO_CACHE
        self.api_version = config["gam_api_version"]
        self.client = ad_manager_client.GetService("ProposalService", self.api_version)
        self.data_client = ad_manager_client.GetDataDownloader(version=self.api_version)
        self.logger = logging.getLogger(__name__)

    @xray_recorder.capture("ProposalClient.create_proposal")
    def create_proposal(self, request: dict) -> SuccessResponse:
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

        for proposal in body.get("proposals", []):
            if (
                "marketplaceInfo" in proposal
                and "buyerAccountId" in proposal["marketplaceInfo"]
            ):
                proposal["marketplaceInfo"]["buyerAccountId"] = self._lookup_buyer_id(
                    proposal["marketplaceInfo"]["buyerAccountId"]
                )

        create_proposal = CreateProposalRequest(body)
        proposal = self.client.createProposals(create_proposal.get_request())
        proposal_response = CreateProposalResponse(proposal).get_response()

        return SuccessResponse(headers, **proposal_response)

    @xray_recorder.capture("ProposalClient.update_proposal")
    def update_proposal(self, request: dict) -> SuccessResponse:
        if type(request) is not dict:
            raise GuardException(
                f"Request must be valid JSON and not type {type(request)}"
            )

        headers = request.get("headers", {})
        if len(headers.keys()) < 1:
            raise GuardException("Expected headers are missing")

        path = request.get("transactionContext", {}).get("path")
        id = path.split("/")[-1]
        if id is None:
            return BadRequest(
                "400001", "Missing Proposal ID in path", headers
            ).get_response()

        body = request.get("body", {})
        if len(body.keys()) < 1:
            raise GuardException("Expected body is missing")

        for proposal in body.get("proposals", []):
            if (
                "marketplaceInfo" in proposal
                and "buyerAccountId" in proposal["marketplaceInfo"]
            ):
                proposal["marketplaceInfo"]["buyerAccountId"] = self._lookup_buyer_id(
                    proposal["marketplaceInfo"]["buyerAccountId"]
                )

        update_proposal = UpdateProposalRequest(self.api_version, id, body)

        response = self.client.getProposalsByStatement(
            update_proposal.get_proposal_statement().ToStatement()
        )
        if "results" not in response and len(response["results"]) < 1:
            raise GuardException("Proposal not found")

        proposal = self.client.updateProposals(
            update_proposal.update_proposal(response["results"][0])
        )
        proposal_response = UpdateProposalResponse(proposal).get_response()

        return SuccessResponse(headers, **proposal_response)

    @xray_recorder.capture("ProposalClient.get_proposals")
    def get_proposals(self, request: dict) -> SuccessResponse:
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
                "400001", "Missing Order ID in path", headers
            ).get_response()

        params = {"id": proposal_id}
        get_proposal = GetProposalsRequest(self.api_version, params)

        proposals = self.client.getProposalsByStatement(
            get_proposal.get_proposal_statement().ToStatement()
        )

        get_proposals_response = GetProposalsResponse(
            proposals["results"]
        ).get_response()

        return SuccessResponse(headers, **get_proposals_response)

    @xray_recorder.capture("ProposalClient.perform_proposal_action")
    def perform_proposal_action(self, request: dict) -> SuccessResponse:
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
            raise GuardException("Expected order ID missing")

        action = request.get("queryStringParameters", {}).get("action")
        if action is None:
            raise GuardException("Expected action is missing")
        if action not in SUPPORTED_ACTIONS.values():
            raise GuardException(f"Unsupported proposal action: {action}")

        perform_proposal_action = PerformProposalActionRequest(
            self.api_version, proposal_id, action
        )
        response = self.client.getProposalsByStatement(
            perform_proposal_action.get_proposal_statement().ToStatement()
        )
        if "results" not in response and len(response["results"]) < 1:
            raise GuardException("Proposal not found")

        result = self.client.performProposalAction(
            perform_proposal_action.get_proposal_action(),
            perform_proposal_action.get_proposal_statement().ToStatement(),
        )

        perform_proposal_action_response = PerformProposalActionResponse(
            result
        ).get_response()

        return SuccessResponse(headers, **perform_proposal_action_response)

    @xray_recorder.capture("ProposalClient._lookup_buyer_id")
    def _lookup_buyer_id(self, provided_buyer_id: str) -> str:
        self.logger.info(
            f"Received buyerAccountId {provided_buyer_id}, looking up in ProgrammaticBuyer"
        )
        # No serverside binding exists so check for non word characters in regex to prevent SQL injection
        if not re.match(r"^\w+$", provided_buyer_id):
            raise GuardException(
                f"Invalid characters in marketplaceInfo.buyerAccountId: {provided_buyer_id}"
            )
        query = (  # nosec - unavoidable with GAM query engine so relying on regex above
            "SELECT BuyerAccountId FROM Programmatic_Buyer "
            f"WHERE (BuyerAccountId='{provided_buyer_id}' OR PartnerClientId='{provided_buyer_id}')"
        )
        response = self.data_client.DownloadPqlResultToList(query)
        if len(response) != 2:
            raise GuardException(
                f"Invalid response for buyerAccountId lookup, expected one match for {provided_buyer_id}, "
                f"got {len(response) - 1}: {json.dumps(response)}"
            )
        self.logger.info(f"Matched buyerAccountId to {response[1][0]}")
        return response[1][0]
