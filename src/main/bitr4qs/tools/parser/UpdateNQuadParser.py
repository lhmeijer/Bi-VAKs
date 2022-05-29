from rdflib.plugins.parsers.nquads import NQuadsParser

from codecs import getreader
from io import StringIO, TextIOBase, BytesIO

# Build up from the NTriples parser:
from rdflib.plugins.parsers.ntriples import ParseError
from rdflib.plugins.parsers.ntriples import r_tail
from rdflib.plugins.parsers.ntriples import r_wspace

__all__ = ["NQuadsParser"]


class UpdateNQuadParser(NQuadsParser):

    def parsestring(self, s, **kwargs):
        """Parse s as an N-Triples string."""
        if not isinstance(s, (str, bytes, bytearray)):
            raise ParseError("Item to parse must be a string instance.")
        if isinstance(s, (bytes, bytearray)):
            f = getreader("utf-8")(BytesIO(s))
        else:
            f = StringIO(s)
        self.parse(f, **kwargs)

    def parse(self, f, bnode_context=None, **kwargs):
        """
        Parse inputsource as an N-Quads file.

        :type inputsource: `rdflib.parser.InputSource`
        :param inputsource: the source of N-Quads-formatted data
        :type bnode_context: `dict`, optional
        :param bnode_context: a dict mapping blank node identifiers to `~rdflib.term.BNode` instances.
                              See `.NTriplesParser.parse`
        """
        if not hasattr(f, "encoding") and not hasattr(f, "charbuffer"):
            # someone still using a bytestream here?
            f = getreader("utf-8")(f)

        if not hasattr(f, "read"):
            raise ParseError("Item to parse must be a file-like object.")

        self.file = f
        self.buffer = ""
        while True:
            self.line = __line = self.readline()
            if self.line is None:
                break
            try:
                self.parseline(bnode_context)
            except ParseError as msg:
                raise ParseError("Invalid line (%s):\n%r" % (msg, __line))

        return self.sink

    def parseline(self, bnode_context=None):

        self.eat(r_wspace)
        if (not self.line) or self.line.startswith(("#")):
            return  # The line is empty or a comment

        subject = self.subject(bnode_context)
        self.eat(r_wspace)

        predicate = self.predicate()
        self.eat(r_wspace)

        obj = self.object(bnode_context)
        self.eat(r_wspace)

        context = self.uriref() or self.nodeid(bnode_context)
        if not context:
            context = None

        self.eat(r_tail)

        if self.line:
            raise ParseError("Trailing garbage")

        self.sink.triple(subject, predicate, obj)
        self.sink.add_modification(graph=context)