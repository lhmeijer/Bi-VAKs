from src.main.bitr4qs.term.Triple import Triple
from rdflib.namespace import RDF
from rdflib.term import URIRef, Literal
from src.main.bitr4qs.namespace import BITR4QS
import hashlib


class Revision(object):

    typeOfRevision = 'Revision'
    nameOfRevision = 'Revision'
    predicateOfPrecedingRevision = BITR4QS.precedingRevision

    def __init__(self, identifier=None,
                 precedingRevision=None,
                 hexadecimalOfHash=None,
                 revisionNumber=None,
                 branchIndex=None):

        self.identifier = identifier
        self._RDFPatterns = [Triple((self._identifier, RDF.type, self.typeOfRevision))]

        self.preceding_revision = precedingRevision
        self._hexadecimalOfHash = hexadecimalOfHash
        self.revision_number = revisionNumber
        self.branch_index = branchIndex

    @property
    def identifier(self):
        return self._identifier

    @identifier.setter
    def identifier(self, identifier):
        if identifier is None:
            self._identifier = BITR4QS.TemporalRevision
            # self._RDFPatterns.append(Triple((self._identifier, RDF.type, self.typeOfRevision)))
        else:
            self._identifier = identifier

    @property
    def preceding_revision(self):
        return self._precedingRevision

    @preceding_revision.setter
    def preceding_revision(self, precedingRevision):
        if precedingRevision is not None:
            self._RDFPatterns.append(Triple((self._identifier, self.predicateOfPrecedingRevision, precedingRevision)))
        self._precedingRevision = precedingRevision

    @property
    def hexadecimal_of_hash(self):
        return self._hexadecimalOfHash

    @hexadecimal_of_hash.setter
    def hexadecimal_of_hash(self, hexadecimalOfHash):
        if hexadecimalOfHash is not None:
            self._RDFPatterns.append(Triple((self._identifier, BITR4QS.hash, hexadecimalOfHash)))
        self._hexadecimalOfHash = hexadecimalOfHash

    @property
    def revision_number(self):
        return self._revisionNumber

    @revision_number.setter
    def revision_number(self, revisionNumber):
        if revisionNumber is not None:
            self._RDFPatterns.append(Triple((self._identifier, BITR4QS.revisionNumber, revisionNumber)))
        self._revisionNumber = revisionNumber

    @property
    def branch_index(self):
        return self._branchIndex

    @branch_index.setter
    def branch_index(self, branchIndex):
        if branchIndex is not None:
            self._RDFPatterns.append(Triple((self._identifier, BITR4QS.branchIndex, branchIndex)))
        self._branchIndex = branchIndex

    def _compute_hash_of_revision(self):
        if self._hexadecimalOfHash is None:
            self._RDFPatterns.sort(key=lambda x: x.n_quad())   # sort RDFPatterns based on lexicographic ordering
            inText = ''.join(triple.n_quad() for triple in self._RDFPatterns)
            hashSHA256 = hashlib.sha256(str.encode(inText))
            return hashSHA256.hexdigest()
        else:
            return self._hexadecimalOfHash

    def _reset_RDFPatterns(self):
        for quad in self._RDFPatterns:
            quad.subject = self._identifier

    def add_to_revision_store(self, revisionStore):
        SPARQLUpdateQuery = """INSERT DATA {{ {0} }}
        """.format('\n'.join(triple.sparql() for triple in self._RDFPatterns))
        revisionStore.revision_store.execute_update_query(SPARQLUpdateQuery)
        # nquads = ''.join(triple.n_quad() for triple in self._RDFPatterns)
        # # print("nquads ", nquads)
        # revisionStore.revision_store.upload_to_dataset(nquads, 'application/n-quads')

    def delete_to_revision_store(self, revisionStore):
        SPARQLUpdateQuery = """DELETE DATA {{ {0} }}
        """.format('\n'.join(triple.sparql() for triple in self._RDFPatterns))
        revisionStore.revision_store.execute_update_query(SPARQLUpdateQuery)

    def generate_identifier(self):
        hashOfRevision = str(self._compute_hash_of_revision())
        identifierOfRevision = self.nameOfRevision + '_' + hashOfRevision
        self.identifier = URIRef(str(BITR4QS) + identifierOfRevision)
        self._reset_RDFPatterns()
        self.hexadecimal_of_hash = Literal(hashOfRevision)

    @classmethod
    def _revision_from_request(cls, request):
        return cls(revisionNumber=request.revision_number)

    @classmethod
    def revision_from_request(cls, request):
        revision = cls._revision_from_request(request)
        revision.generate_identifier()
        return revision

    @classmethod
    def _revision_from_data(cls, **data):

        assert 'revisionNumber' in data, "revisionNumber should be in the data of the revision"
        assert 'precedingRevision' in data, "precedingRevision should be in the data of the revision"

        return cls(**data)

    @classmethod
    def revision_from_data(cls, **data):
        revision = cls._revision_from_data(**data)
        revision.generate_identifier()
        return revision

    def __dict__(self):
        result = {'identifier': str(self._identifier), 'hexadecimalOfHash': str(self._hexadecimalOfHash)}
        if self._precedingRevision is not None:
            result['precedingRevision'] = str(self._precedingRevision)
        if self._revisionNumber is not None:
            result['revisionNumber'] = str(self._revisionNumber)
        if self._branchIndex is not None:
            result['branchIndex'] = str(self._branchIndex)
        return result

