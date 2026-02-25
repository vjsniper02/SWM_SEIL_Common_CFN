# Facilitates a Programmatic Guaranteed UpdatePoposalLineItems request to Google Ad Manager client.
import re
from gam_core.model.guard_exception import GuardException
from googleads import ad_manager
from gam_core.model.proposal_lineitem import ProposalLineItem

ID_STRING_CHECK_PATTERN = re.compile(r"[^0-9,]")


class UpdateProposalLineItemRequest:
    def __init__(self, api_version, request_body):
        request_proposal_lineitems = request_body.get("proposalLineitems", [])
        if len(request_proposal_lineitems) < 1:
            raise GuardException("No proposal line item provided")
        proposal_lineitems = {}
        for request_proposal_lineitem in request_proposal_lineitems:
            if (
                request_proposal_lineitem["id"] == None
                or request_proposal_lineitem["id"] == ""
            ):
                raise GuardException("Missing proposal line items ID")
            lineitem_delta = ProposalLineItem(**request_proposal_lineitem).get_request()
            proposal_lineitems[lineitem_delta["id"]] = lineitem_delta

        ids = list(proposal_lineitems.keys())
        idstring = ",".join(str(id) for id in ids)
        if re.search(ID_STRING_CHECK_PATTERN, idstring):
            raise GuardException(
                "IDs contain invalid characters, only numbers are allowed"
            )

        self.proposal_lineitems = proposal_lineitems
        self.statement = (
            ad_manager.StatementBuilder(version=api_version)
            .Where("id in (:ids)")
            .WithBindVariable("ids", ids)
        )

    def get_proposal_lineitem(self):
        return self.proposal_lineitems

    def update_proposal_lineitem(self, old_proposal_lineitems):
        try:
            updated_proposal_lineitems = []
            for old_proposal_lineitem in old_proposal_lineitems:
                if not old_proposal_lineitem["isArchived"]:
                    proposal_lineitem_delta = self.proposal_lineitems.get(
                        old_proposal_lineitem["id"], None
                    )
                    if proposal_lineitem_delta != None:
                        updated_delta = {}
                        for key in proposal_lineitem_delta.keys():
                            if (
                                proposal_lineitem_delta[key] != ""
                                and proposal_lineitem_delta[key] != None
                            ):
                                updated_delta[key] = proposal_lineitem_delta.get(key)
                        updated_proposal_lineitems.append(updated_delta)
                    else:
                        raise GuardException(
                            f"Error while updating ProposalLineItem ID: {old_proposal_lineitem['id']}"
                        )

        except:
            raise GuardException("Invalid proposal line item to update")

        return updated_proposal_lineitems

    def get_proposal_lineitem_statement(self):
        return self.statement
