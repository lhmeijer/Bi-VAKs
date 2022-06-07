from .Request import Request
from rdflib.term import URIRef, Literal
from rdflib.namespace import XSD
from src.main.bitr4qs.revision.InitialRevision import InitialRevision
from src.main.bitr4qs.revision.Snapshot import Snapshot
import datetime
from src.main.bitr4qs.store.HttpQuadStore import HttpQuadStore
from src.main.bitr4qs.tools.parser.UpdateNQuadParser import UpdateNQuadParser
from src.main.bitr4qs.revision.Update import Update


class InitialRequest(Request):

    type = 'initial'

    def __init__(self, request):
        super().__init__(request)

        self._nameDataset = None
        self._urlDataset = None
        self._effectiveDate = None

    def evaluate_request(self, revisionStore):

        # Obtain the author
        author = self._request.values.get('author', None) or None
        if author is not None:
            self._author = Literal(author)
        else:
            # TODO author is not given return an error
            pass

        # Obtain the description
        description = self._request.values.get('description', None) or None
        if description is not None:
            self._description = Literal(description)
        else:
            # TODO description is not given return an error
            pass

        # Obtain the creation date of the transaction revision
        time = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S+02:00")
        self._creationDate = Literal(str(time), datatype=XSD.dateTimeStamp)

        # Check whether revision store is empty
        # if it is not empty return an error because it should be the first transaction revision

        # Obtain the name of an already existing dataset
        nameDataset = self._request.values.get('nameDataset', None) or None
        if nameDataset is not None:
            self._nameDataset = Literal(nameDataset)

        # Obtain the url of an already existing dataset
        urlDataset = self._request.values.get('urlDataset', None) or None
        if urlDataset is not None:
            self._urlDataset = Literal(urlDataset)

        # Obtain start date
        startDate = self._request.values.get('startDate', None) or None
        if startDate is not None:
            if startDate == 'unknown':
                self._startDate = None
            else:
                self._startDate = Literal(str(startDate), datatype=XSD.dateTimeStamp)

        # Obtain end date
        endDate = self._request.values.get('endDate', None) or None
        if endDate is not None:
            if endDate == 'unknown':
                self._endDate = None
            else:
                self._endDate = Literal(str(endDate), datatype=XSD.dateTimeStamp)

        self._revisionNumber = revisionStore.new_revision_number()
        self._branchIndex = revisionStore.new_branch_index()

    def transaction_revision_from_request(self):
        revision = InitialRevision.revision_from_data(creationDate=self._creationDate, description=self._description,
                                                      author=self._author, revisionNumber=self._revisionNumber)
        self._currentTransactionRevision = revision.identifier
        return revision

    def valid_revisions_from_request(self):
        # Check whether the user already uses an existing dataset, and create a snapshot and update from it.
        if self._nameDataset and self._urlDataset:
            datastore = HttpQuadStore(self._nameDataset, self._urlDataset)
            response = datastore.get_dataset('application/n-quads').split('\n')

            # Parse the N-Quads to a list of triples or quads
            update = Update(identifier=None, startDate=self._startDate, endDate=self._endDate,
                            branchIndex=self._branchIndex, revisionNumber=self._revisionNumber)
            updateParser = UpdateNQuadParser(sink=update)
            for nquad in response:
                updateParser.parsestring(nquad)
            update.generate_identifier()
            return [update]
        else:
            return []
