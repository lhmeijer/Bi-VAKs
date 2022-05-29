
class Error(Exception):
    pass


class InvalidConfigurationError(Error):
    pass


class MissingConfigurationError(Error):
    pass


class UnsupportedQuery(Exception):
    pass


class SparqlProtocolError(Error):
    pass


class NonAbsoluteBaseError(Error):
    pass


class SPARQLConnectorException(Exception):
    pass


class BiTR4QsError(Error):

    message = "A Bi-TR4Qs exception has occurred."

    def __init__(self, extraInformation):
        if extraInformation:
            formatted_msg = "%s: %s. %s" % (self.__class__.__name__, self.message, extraInformation)
        else:
            formatted_msg = "%s: %s." % (self.__class__.__name__, self.message)
        super(BiTR4QsError, self).__init__(formatted_msg)


class MissingInformationError(BiTR4QsError):

    message = "Not enough information is given to set up the revisions."


class RevisionConstructionError(BiTR4QsError):

    message = "The revision cannot be constructed."



class SPARQLException(Exception):

    message = "An SPARQL exception has occurred."

    def __init__(self, response=None):
        if response:
            formatted_msg = "%s: %s. \n\nResponse:\n%s" % (self.__class__.__name__, self.message, response)
        else:
            formatted_msg = "%s: %s." % (self.__class__.__name__, self.message)

        super(SPARQLException, self).__init__(formatted_msg)


class EndPointInternalError(SPARQLException):
    """
    Exception type for Internal Server Error responses. Usually HTTP response status code 500.
    """

    message = "Endpoint returned code 500 and the service fails or refuses to execute the query."


class QueryBadFormed(SPARQLException):
    """
    Query Bad Formed exception. Usually HTTP response status code 400.
    """

    message = "A bad request has been sent to the endpoint, probably the sparql query is bad formed"


class EndPointNotFound(SPARQLException):
    """
    End Point Not Found exception. Usually HTTP response status code 404.
    """

    message = "It was impossible to connect with the endpoint in that address, check if it is correct"


class Unauthorized(SPARQLException):
    """
    Access is denied due to invalid credentials (unauthorized). Usually HTTP response status code 401.
    """

    message = "Access is denied due to invalid credentials (unauthorized). Check the credentials"


class URITooLong(SPARQLException):
    """
    The URI requested by the client is longer than the server is willing to interpret. HTTP response status code 414
    """

    message = "The URI requested by the client is longer than the server is willing to interpret. " \
              "Check if the request was sent using GET method instead of POST method."

