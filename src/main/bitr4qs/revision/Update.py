from .ValidRevision import ValidRevision
from rdflib.term import Literal
from src.main.bitr4qs.term.Triple import Triple
from src.main.bitr4qs.term.Modification import Modification
from src.main.bitr4qs.term.RDFStarTriple import RDFStarTriple
from src.main.bitr4qs.namespace import BITR4QS
from src.main.bitr4qs.term.RDFStarQuad import RDFStarQuad
from src.main.bitr4qs.term.Quad import Quad
from .TransactionRevision import TransactionRevision


class UpdateRevision(TransactionRevision):

    typeOfRevision = BITR4QS.UpdateRevision
    nameOfRevision = 'UpdateRevision'


class Update(ValidRevision):

    typeOfRevision = BITR4QS.Update
    nameOfRevision = 'Update'
    predicateOfPrecedingRevision = BITR4QS.precedingUpdate

    def __init__(self, identifier=None, precedingRevision=None, hexadecimalOfHash=None, modifications=None,
                 startDate=None, endDate=None, revisionNumber=None, branchIndex=None):

        super().__init__(identifier=identifier, precedingRevision=precedingRevision, branchIndex=branchIndex,
                         hexadecimalOfHash=hexadecimalOfHash, revisionNumber=revisionNumber)
        self.modifications = modifications
        self.start_date = startDate
        self.end_date = endDate

        self._mostRecentTriple = None

    @property
    def modifications(self):
        return self._modifications

    @modifications.setter
    def modifications(self, modifications):
        if modifications and len(modifications) > 0:
            self._RDFPatterns.extend([self._to_rdf_star(modification) for modification in modifications])
        self._modifications = modifications

    @property
    def start_date(self) -> Literal:
        return self._startDate

    @start_date.setter
    def start_date(self, startDate: Literal):
        if startDate:
            self._RDFPatterns.append(Triple((self._identifier, BITR4QS.startedAt, startDate)))
        self._startDate = startDate

    @property
    def end_date(self):
        return self._endDate

    @end_date.setter
    def end_date(self, endDate):
        if endDate:
            self._RDFPatterns.append(Triple((self._identifier, BITR4QS.endedAt, endDate)))
        self._endDate = endDate

    def triple(self, s, p, o):
        self._mostRecentTriple = (s, p, o)

    def add_modification(self, deletion=False, graph=None):
        modification = Modification(Triple(self._mostRecentTriple) if graph is None else
                                    Quad(self._mostRecentTriple, graph), deletion)

        if self._modifications is None:
            self._modifications = []

        self._RDFPatterns.append(self._to_rdf_star(modification))
        self._modifications.append(modification)
        return None

    def _to_rdf_star(self, modification):
        if modification.deletion:
            if isinstance(modification.value, Quad):
                value = RDFStarQuad((self._identifier, BITR4QS.deletes, modification.value))
            elif isinstance(modification.value, Triple):
                value = RDFStarTriple((self._identifier, BITR4QS.deletes, modification.value))
            else:
                raise Exception
        else:
            if isinstance(modification.value, Quad):
                value = RDFStarQuad((self._identifier, BITR4QS.inserts, modification.value))
            elif isinstance(modification.value, Triple):
                value = RDFStarTriple((self._identifier, BITR4QS.inserts, modification.value))
            else:
                raise Exception
        return value

    @classmethod
    def _revision_from_data(cls, **data):
        # print("data ", data)
        assert 'revisionNumber' in data, "revisionNumber should be in the data of the revision"
        assert 'modifications' in data, "modifications should be in the data of the revision"
        assert 'startDate' in data, "startDate should be in the data of the revision"
        assert 'endDate' in data, "endDate should be in the data of the revision"
        assert 'branchIndex' in data, "branchIndex should be in the data of the revision"

        return cls(**data)

    def __dict__(self):
        result = super().__dict__()
        result['startDate'] = str(self._startDate)
        result['endDate'] = str(self._endDate)
        result['modifications'] = [str(modification) for modification in self._modifications]
        return result

    def modify(self, revisionStore, headRevision, otherModifications=None, otherStartDate=None, otherEndDate=None,
               revisionNumber=None, branchIndex=None, relatedContent=False, shouldBeTested=None):

        startDate = otherStartDate if otherStartDate is not None else self._startDate
        endDate = otherEndDate if otherEndDate is not None else self._endDate

        # Nothing has been changed so no need for a modified update
        # if startDate == self._startDate and endDate == self._endDate and not otherModifications:
        #     return None

        # Check if the update content is related
        if relatedContent:
            self._modifications = revisionStore.preceding_modifications(self._identifier)

        newModifications = []
        if otherModifications:

            transactionRevision = revisionStore.transaction_from_valid_and_valid_from_transaction(
                revisionID=self._identifier, transactionFromValid=True, revisionType='update')

            # Check whether the existing modifications can be changed
            for otherModification in otherModifications:
                canBeAdded = False
                index = None
                for i in range(len(self._modifications)):
                    if otherModification.value == self._modifications[i].value:
                        if otherModification.deletion and self._modifications[i].insertion:
                            canBeAdded = revisionStore.can_quad_be_modified(
                                quad=otherModification.value, revisionA=headRevision, endDate=endDate, deletion=False,
                                revisionB=transactionRevision.identifier, startDate=startDate)
                            index = i
                        elif otherModification.insertion and self._modifications[i].deletion:
                            canBeAdded = revisionStore.can_quad_be_modified(
                                quad=otherModification.value, revisionA=headRevision, endDate=endDate, deletion=True,
                                revisionB=transactionRevision.identifier, startDate=startDate)
                            index = i
                        elif otherModification.deletion and self._modifications[i].deletion:
                            canBeAdded = False
                            index = i
                        else:
                            canBeAdded = False
                            index = i

                if not index:
                    canBeAdded = revisionStore.can_quad_be_added_or_deleted(
                        quad=otherModification.value, headRevision=headRevision, startDate=startDate, endDate=endDate,
                        deletion=otherModification.deletion)
                    if canBeAdded:
                        newModifications.append(otherModification)
                elif index and canBeAdded:
                    _ = self._modifications.pop(index)
                    newModifications.append(otherModification)
                else:
                    raise Exception("Modified quad cannot exist in this update.")

        # Determine whether we should test that the modifications can be added or deleted from the revision store.
        if shouldBeTested is not None:
            try:
                canBeModified = revisionStore.can_modifications_be_added_or_deleted(
                    modifications=self._modifications, headRevision=headRevision, startDate=self._startDate,
                    endDate=self._endDate, revisionID=self._identifier)
            except Exception as e:
                raise e
            # If shouldBeTest == 'no' we only run the test, but we do not mind the answer. -> Compute ingestion time
            if not canBeModified and shouldBeTested == 'yes':
                raise Exception('Quads or triples cannot exist in this update with this new start or end date.')

        # for modification in self._modifications:
        #     canBeModified = revisionStore.can_quad_be_added_or_deleted(
        #         quad=modification.value, headRevision=headRevision, startDate=startDate, endDate=endDate,
        #         deletion=modification.deletion, revisionID=self._identifier)
        #     if shouldBeTested and not canBeModified:
        #         raise Exception("Quads or triples cannot exist in this update with this new start or end date.")

        if relatedContent:
            modifications = newModifications
        else:
            modifications = self._modifications + newModifications

        modifiedUpdate = Update.revision_from_data(
            modifications=modifications, branchIndex=branchIndex, startDate=startDate, endDate=endDate,
            revisionNumber=revisionNumber, precedingRevision=self._identifier)

        return modifiedUpdate

    def revert(self, revisionStore, headRevision, revisionNumber=None, branchIndex=None, relatedContent=True):
        # Check whether there exists a preceding update
        if self._precedingRevision is not None:
            # Get the preceding update
            otherUpdate = revisionStore.preceding_revision(self._identifier, revisionType='update')
            if relatedContent:
                for modification in self._modifications:
                    modification.invert()
            else:
                previousModifications = []
                for otherModification in otherUpdate.modifications:
                    index = None
                    for i in range(len(self._modifications)):
                        if otherModification == self._modifications[i]:
                            index = i
                    if index:
                        _ = self._modifications.pop(index)
                    else:
                        previousModifications.append(otherModification)
                self._modifications.extend(previousModifications)

            revertedUpdate = self.modify(revisionNumber=revisionNumber, branchIndex=branchIndex,
                                         otherStartDate=otherUpdate.start_date, otherEndDate=otherUpdate.end_date,
                                         otherModifications=self._modifications, headRevision=headRevision,
                                         relatedContent=relatedContent, revisionStore=revisionStore)
        else:
            if relatedContent:
                precedingModifications = revisionStore.preceding_modifications(self._identifier)
                self._modifications.extend(precedingModifications)
                for modification in self._modifications:
                    modification.invert()
            else:
                self._modifications = None
            revertedUpdate = Update.revision_from_data(revisionNumber=revisionNumber, branchIndex=branchIndex,
                                                       startDate=None, endDate=None, modifications=self._modifications,
                                                       precedingRevision=self._identifier)
        return revertedUpdate
