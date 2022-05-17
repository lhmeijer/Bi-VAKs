from src.main.bitr4qs.term.Triple import Triple
from rdflib.namespace import RDF
from rdflib.term import URIRef, Literal
from src.main.bitr4qs.namespace import BITR4QS
import hashlib


class Revision(object):

    typeOfRevision = 'Revision'
    nameOfRevision = 'Revision'
    predicateOfPrecedingRevision = 'precedingRevision'

    def __init__(self, identifier=None,
                 precedingRevision=None,
                 hexadecimalOfHash=None,
                 revisionNumber=None):

        self._RDFPatterns = []

        self.identifier = identifier
        self.preceding_revision = precedingRevision
        self._hexadecimalOfHash = hexadecimalOfHash
        self.revision_number = revisionNumber

    @property
    def identifier(self):
        return self._identifier

    @identifier.setter
    def identifier(self, identifier):
        if identifier is None:
            self._identifier = BITR4QS.TemporalRevision
            self._RDFPatterns.append(Triple((self._identifier, RDF.type, self.typeOfRevision)))
        else:
            self._identifier = identifier
        print(self._identifier)

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

    def compute_hash_of_revision(self):
        if self._hexadecimalOfHash is None:
            self._RDFPatterns.sort(key=lambda x: x.n_quad())   # sort RDFPatterns based on lexicographic ordering
            inText = ''.join(triple.n_quad() for triple in self._RDFPatterns)
            hashSHA256 = hashlib.sha256(str.encode(inText))
            return hashSHA256.hexdigest()
        else:
            return self._hexadecimalOfHash

    def reset_RDFPatterns(self):
        for quad in self._RDFPatterns:
            quad.subject = self._identifier

    def add_to_revision_store(self, revisionStore):
        SPARQLUpdateQuery = """INSERT DATA {{ {0} }}
        """.format('\n'.join(triple.to_sparql() for triple in self._RDFPatterns))
        print(SPARQLUpdateQuery)

    def delete_to_revision_store(self, revisionStore):
        SPARQLUpdateQuery = """DELETE DATA {{ {0} }}
        """.format('\n'.join(triple.to_sparql() for triple in self._RDFPatterns))
        print(SPARQLUpdateQuery)

    @classmethod
    def _revision_from_request(cls, request):
        return cls(revisionNumber=request.revision_number)

    @classmethod
    def revision_from_request(cls, request):
        revision = cls._revision_from_request(request)
        hashOfRevision = str(revision.compute_hash_of_revision())
        identifierOfRevision = revision.nameOfRevision + '_' + hashOfRevision
        revision.identifier = URIRef(str(BITR4QS) + identifierOfRevision)
        revision.reset_RDFPatterns()
        revision.hexadecimal_of_hash = Literal(hashOfRevision)
        return revision

    @classmethod
    def _revision_from_data(cls, **data):
        assert 'revisionNumber' in data, "revisionNumber should be in the data of the revision"
        assert 'precedingRevision' in data, "precedingRevision should be in the data of the revision"

        return cls(data)

    @classmethod
    def revision_from_data(cls, **data):
        revision = cls._revision_from_data(**data)
        hashOfRevision = str(revision.compute_hash_of_revision())
        identifierOfRevision = revision.nameOfRevision + '_' + hashOfRevision
        revision.identifier = URIRef(str(BITR4QS) + identifierOfRevision)
        revision.reset_RDFPatterns()
        revision.hexadecimal_of_hash = Literal(hashOfRevision)
        return revision

