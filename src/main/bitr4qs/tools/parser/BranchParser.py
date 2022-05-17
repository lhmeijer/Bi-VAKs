from .Parser import Parser
from src.main.bitr4qs.namespace import BITR4QS
from rdflib.term import URIRef


class BranchParser(Parser):

    @staticmethod
    def _get_valid_revision(identifier):
        from src.main.bitr4qs.revision.Branch import Branch
        return Branch(URIRef(identifier))

    @staticmethod
    def _parse_valid_revision(revision, p, o):

        if str(p) == str(BITR4QS.branchName):
            revision.branch_name = o

        elif str(p) == str(BITR4QS.branchedOffAt):
            revision.branched_off_at = o

        elif str(p) == str(BITR4QS.precedingBranch):
            revision.preceding_revision = o

    @staticmethod
    def _get_transaction_revision(identifier):
        from src.main.bitr4qs.revision.BranchRevision import BranchRevision
        return BranchRevision(URIRef(identifier))

    @staticmethod
    def _parse_transaction_revision(revision, p, o):

        if str(p) == str(BITR4QS.branch):
            revision.valid_revision = o
