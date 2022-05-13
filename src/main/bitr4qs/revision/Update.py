from .ValidRevision import ValidRevision
from rdflib.term import URIRef, Literal
from src.main.bitr4qs.term.Triple import Triple
from src.main.bitr4qs.term.Modification import Modification
from src.main.bitr4qs.term.RDFStarTriple import RDFStarTriple
from src.main.bitr4qs.namespace import BITR4QS
from src.main.bitr4qs.term.RDFStarQuad import RDFStarQuad
from src.main.bitr4qs.term.Quad import Quad


class Update(ValidRevision):

    typeOfRevision = BITR4QS.Update
    nameOfRevision = 'Update'

    def __init__(self, identifier=None,
                 precedingRevision=None,
                 hexadecimalOfHash=None,
                 modifications=None,
                 startDate=None,
                 endDate=None,
                 revisionNumber=None,
                 branchIndex=None):
        super().__init__(identifier=identifier, precedingRevision=precedingRevision,
                         hexadecimalOfHash=hexadecimalOfHash, revisionNumber=revisionNumber, branchIndex=branchIndex)
        self.modifications = modifications
        self.start_date = startDate
        self.end_date = endDate

        self._mostRecentTriple = None

    @property
    def modifications(self):
        return self._modifications

    @modifications.setter
    def modifications(self, modifications):
        if modifications is not None:
            self._RDFPatterns.extend([self._to_rdf_star(modification) for modification in modifications])
        self._modifications = modifications

    @property
    def start_date(self):
        return self._startDate

    @start_date.setter
    def start_date(self, startDate):
        if startDate is not None:
            self._RDFPatterns.append(Triple((self._identifier, BITR4QS.startedAt, startDate)))
        self._startDate = startDate

    @property
    def end_date(self):
        return self._endDate

    @end_date.setter
    def end_date(self, endDate):
        if endDate is not None:
            self._RDFPatterns.append(Triple((self._identifier, BITR4QS.endedAt, endDate)))
        self._endDate = endDate

    def triple(self, s, p, o):
        self._mostRecentTriple = (s, p, o)

    def add_modification(self, deletion=False, graph=None):
        modification = Modification(Triple(self._mostRecentTriple) if graph is None else
                                    Quad(self._mostRecentTriple, graph), deletion)

        if self._modifications is None:
            self._modifications = []

        self._modifications.append(modification)

    def _to_rdf_star(self, modification):
        if modification.deletion:
            if isinstance(modification.value, Quad):
                value = RDFStarQuad((self._identifier, BITR4QS.deletes, modification.value))
            elif isinstance(modification.value, Triple):
                value = RDFStarTriple((self._identifier, BITR4QS.deletes, modification.value))
            else:
                pass
        else:
            if isinstance(modification.value, Quad):
                value = RDFStarQuad((self._identifier, BITR4QS.inserts, modification.value))
            elif isinstance(modification.value, Triple):
                value = RDFStarTriple((self._identifier, BITR4QS.inserts, modification.value))
            else:
                pass
        return value

    @classmethod
    def _revision_from_request(cls, request):
        return cls(modifications=request.modifications, startDate=request.start_date, endDate=request.end_date,
                   precedingRevision=request.preceding_valid_revision, revisionNumber=request.revision_number,
                   branchIndex=request.branch_index)
