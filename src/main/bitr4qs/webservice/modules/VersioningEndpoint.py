from src.main.bitr4qs.core.BiTR4Qs import BiTR4QsSingleton
from src.main.bitr4qs.namespace import BITR4QS
from flask import Blueprint, request, make_response, current_app
import src.main.bitr4qs.request as requests
from rdflib.term import Literal, URIRef


versioningEndpoint = Blueprint('versioning_endpoint', __name__)


@versioningEndpoint.route("/update/<path:updateID>", methods=['POST'])
def update(updateID):
    BiTR4QsConfiguration = current_app.config['BiTR4QsConfiguration']
    BiTR4QsCore = BiTR4QsSingleton.get(BiTR4QsConfiguration)

    if BiTR4QsConfiguration.related_update_content():
        updateRequest = requests.ModifiedRelatedUpdateRequest(request)
    elif BiTR4QsConfiguration.repeated_update_content():
        updateRequest = requests.ModifiedRepeatedUpdateRequest(request)
    else:
        return make_response('No update content strategy is given', 400)

    try:
        updateID = URIRef(str(BITR4QS) + updateID)
        update = BiTR4QsCore.modify_versioning_operation(updateID, updateRequest)
        response = make_response('', 200)
        return response
    except Exception as e:
        return make_response('Error after executing the update query.', 400)


@versioningEndpoint.route("/initialise", methods=['POST'])
def initialise():
    BiTR4QsConfiguration = current_app.config['BiTR4QsConfiguration']
    BiTR4QsCore = BiTR4QsSingleton.get(BiTR4QsConfiguration)
    initialRequest = requests.InitialRequest(request)

    try:
        initial = BiTR4QsCore.apply_versioning_operation(initialRequest)
        response = make_response('', 200)
        return response
    except Exception as e:
        return make_response('Error after executing the tag query.', 400)


@versioningEndpoint.route("/tag", defaults={'tagID': None}, methods=['POST'])
@versioningEndpoint.route("/tag/<path:tagID>", methods=['POST'])
def tag(tagID):
    BiTR4QsConfiguration = current_app.config['BiTR4QsConfiguration']
    BiTR4QsCore = BiTR4QsSingleton.get(BiTR4QsConfiguration)
    tagRequest = requests.TagRequest(request)

    if tagID is not None:
        try:
            tag = BiTR4QsCore.modify_versioning_operation(tagID, tagRequest)
            response = make_response('', 200)
            return response
        except Exception as e:
            return make_response('Error after executing the tag query.', 400)
    else:
        try:
            tag = BiTR4QsCore.apply_versioning_operation(tagRequest)
            response = make_response('', 200)
            return response
        except Exception as e:
            return make_response('Error after executing the tag query.', 400)


@versioningEndpoint.route("/snapshot", defaults={'snapshotID': None}, methods=['POST'])
@versioningEndpoint.route("/snapshot/<path:snapshotID>", methods=['POST'])
def snapshot(snapshotID):
    BiTR4QsConfiguration = current_app.config['BiTR4QsConfiguration']
    BiTR4QsCore = BiTR4QsSingleton.get(BiTR4QsConfiguration)
    snapshotRequest = requests.SnapshotRequest(request)

    if snapshotID is not None:
        try:
            tag = BiTR4QsCore.modify_versioning_operation(tagID, tagRequest)
            response = make_response('', 200)
            return response
        except Exception as e:
            return make_response('Error after executing the tag query.', 400)
    else:
        try:
            tag = BiTR4QsCore.apply_versioning_operation(tagRequest)
            response = make_response('', 200)
            return response
        except Exception as e:
            return make_response('Error after executing the tag query.', 400)

    try:
        snapshot = BiTR4QsCore.apply_versioning_operation(snapshotRequest)
        response = make_response('', 200)
        return response
    except Exception as e:
        return make_response('Error after executing the snapshot query.', 400)


@versioningEndpoint.route("/branch", defaults={'branchID': None}, methods=['POST'])
@versioningEndpoint.route("/branch/<path:branchID>", methods=['POST'])
def branch(branchID):
    BiTR4QsConfiguration = current_app.config['BiTR4QsConfiguration']
    BiTR4QsCore = BiTR4QsSingleton.get(BiTR4QsConfiguration)
    branchRequest = requests.BranchRequest(request)

    if snapshotID is not None:
        try:
            tag = BiTR4QsCore.modify_versioning_operation(tagID, tagRequest)
            response = make_response('', 200)
            return response
        except Exception as e:
            return make_response('Error after executing the tag query.', 400)
    else:
        try:
            tag = BiTR4QsCore.apply_versioning_operation(tagRequest)
            response = make_response('', 200)
            return response
        except Exception as e:
            return make_response('Error after executing the tag query.', 400)

    try:
        branch = BiTR4QsCore.apply_versioning_operation(branchRequest)
        response = make_response('', 200)
        return response
    except Exception as e:
        return make_response('Error after executing the branch query.', 400)


@versioningEndpoint.route("/revert", defaults={'revisionID': None}, methods=['POST'])
@versioningEndpoint.route("/revert/<path:revisionID>", methods=['POST'])
def revert(revisionID):
    BiTR4QsConfiguration = current_app.config['BiTR4QsConfiguration']
    BiTR4QsCore = BiTR4QsSingleton.get(BiTR4QsConfiguration)
    revertRequest = requests.RevertRequest(request)

    try:
        revert = BiTR4QsCore.revert_versioning_operation(revisionID, revertRequest)
        response = make_response('', 200)
        return response
    except Exception as e:
        return make_response('Error after executing the branch query.', 400)
