from src.main.bitr4qs.core.BiTR4Qs import BiTR4QsSingleton
from src.main.bitr4qs.namespace import BITR4QS
from flask import Blueprint, request, make_response, current_app, jsonify
import src.main.bitr4qs.request as requests
from rdflib.term import Literal, URIRef


versioningEndpoint = Blueprint('versioning_endpoint', __name__)


@versioningEndpoint.route("/update/<path:updateID>", methods=['POST'])
def update(updateID):
    BiTR4QsConfiguration = current_app.config['BiTR4QsConfiguration']

    if BiTR4QsConfiguration.related_update_content():
        updateRequest = requests.ModifiedRelatedUpdateRequest(request)
    elif BiTR4QsConfiguration.repeated_update_content():
        updateRequest = requests.ModifiedRepeatedUpdateRequest(request)
    else:
        return make_response('No update content strategy is given', 400)

    return versioning_operation(revisionRequest=updateRequest, revisionID=updateID)


@versioningEndpoint.route("/initialise", methods=['POST'])
def initialise():
    BiTR4QsConfiguration = current_app.config['BiTR4QsConfiguration']
    BiTR4QsCore = BiTR4QsSingleton.get(BiTR4QsConfiguration)
    print("BiTR4QsCore ", BiTR4QsCore)
    initialRequest = requests.InitialRequest(request)

    try:
        initial = BiTR4QsCore.apply_versioning_operation(initialRequest)
        response = make_response('success', 200)
        response.headers['X-CurrentRevision'] = initialRequest.current_transaction_revision
        if initialRequest.revision_number is not None:
            response.headers['X-CurrentRevisionNumber'] = initialRequest.revision_number
        return response
    except Exception as e:
        return make_response('Error after executing the initialise query.', 400)


@versioningEndpoint.route("/tag", defaults={'tagID': None}, methods=['POST'])
@versioningEndpoint.route("/tag/<path:tagID>", methods=['POST'])
def tag(tagID):
    tagRequest = requests.TagRequest(request)
    return versioning_operation(revisionRequest=tagRequest, revisionID=tagID)


@versioningEndpoint.route("/snapshot", defaults={'snapshotID': None}, methods=['POST'])
@versioningEndpoint.route("/snapshot/<path:snapshotID>", methods=['POST'])
def snapshot(snapshotID):
    snapshotRequest = requests.SnapshotRequest(request)
    return versioning_operation(revisionRequest=snapshotRequest, revisionID=snapshotID)


@versioningEndpoint.route("/branch", defaults={'branchID': None}, methods=['POST'])
@versioningEndpoint.route("/branch/<path:branchID>", methods=['POST'])
def branch(branchID):
    branchRequest = requests.BranchRequest(request)
    return versioning_operation(revisionRequest=branchRequest, revisionID=branchID)


@versioningEndpoint.route("/revert", defaults={'revisionID': None}, methods=['POST'])
@versioningEndpoint.route("/revert/<path:revisionID>", methods=['POST'])
def revert(revisionID):
    BiTR4QsConfiguration = current_app.config['BiTR4QsConfiguration']
    BiTR4QsCore = BiTR4QsSingleton.get(BiTR4QsConfiguration)
    revertRequest = requests.RevertRequest(request)

    try:
        revisionID = URIRef(str(BITR4QS) + revisionID)
        revert = BiTR4QsCore.revert_versioning_operation(revisionID, revertRequest)
        response = make_response('', 200)
        return response
    except Exception as e:
        return make_response('Error after executing the branch query.', 400)


def versioning_operation(revisionRequest, revisionID=None):
    """

    :param revisionRequest:
    :param revisionID:
    :return:
    """
    BiTR4QsConfiguration = current_app.config['BiTR4QsConfiguration']
    BiTR4QsCore = BiTR4QsSingleton.get(BiTR4QsConfiguration)

    if revisionID:
        try:
            revisionID = URIRef(str(BITR4QS) + revisionID)
            revisions = BiTR4QsCore.modify_versioning_operation(revisionID, revisionRequest)
        except Exception as e:
            return make_response('Error after executing the tag query.', 400)
    else:
        try:
            revisions = BiTR4QsCore.apply_versioning_operation(revisionRequest)
        except Exception as e:
            print("e ", e)
            return make_response('Error after executing the tag query.', 400)

    response = make_response(jsonify(revisions[0]), 200)
    response.headers['X-CurrentRevision'] = str(revisionRequest.current_transaction_revision)
    if revisionRequest.revision_number is not None:
        response.headers['X-CurrentRevisionNumber'] = str(revisionRequest.revision_number)
    if revisionRequest.branch is not None:
        response.headers['X-Branch'] = str(revisionRequest.branch)
    return response
