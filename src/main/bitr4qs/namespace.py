from rdflib.term import URIRef
from rdflib.namespace import DefinedNamespace, Namespace


class BITR4QS(DefinedNamespace):
    """
    Bi-TR4Qs vocabulary
    """

    # http://www.w3.org/1999/02/22-rdf-syntax-ns#Property
    createdAt: URIRef  # The creation date of a Transaction Revision.
    precedingRevision: URIRef
    startedAt: URIRef
    endedAt: URIRef
    validAt: URIRef
    transactedAt: URIRef
    branch: URIRef
    inserts: URIRef
    deletes: URIRef
    author: URIRef
    revisionNumber: URIRef
    branchIndex: URIRef
    tag: URIRef
    merge: URIRef
    snapshot: URIRef
    update: URIRef
    revert: URIRef
    hash: URIRef
    precedingSnapshot: URIRef
    precedingUpdate: URIRef
    precedingTag: URIRef
    precedingBranch: URIRef
    precedingRevert: URIRef
    nameDataset: URIRef
    urlDataset: URIRef
    tagName: URIRef
    branchName: URIRef
    branchedOffAt: URIRef

    # http://www.w3.org/2000/01/rdf-schema#Class
    UpdateRevision: URIRef
    Update: URIRef
    SnapshotRevision: URIRef
    Snapshot: URIRef
    TagRevision: URIRef
    Tag: URIRef
    BranchRevision: URIRef
    Branch: URIRef
    TemporalRevision: URIRef
    TransactionRevision: URIRef
    ValidRevision: URIRef
    HeadRevision: URIRef
    InitialRevision: URIRef
    RevertRevision: URIRef
    Revert: URIRef

    _NS = Namespace("http://bi-tr4qs.org/vocab/")