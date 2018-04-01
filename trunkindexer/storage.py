import io
from collections import UserDict
import datetime
import json
import os

import elasticsearch

import pytz
from tzlocal import get_localzone


class Call(UserDict, io.FileIO):
    """An object that represents a record call and associated metadata

    c = Call('/tmp/some.wav')
    c.read() # returns wav data

    c['created'] # returns the created property

    c['some key'] = 'Some value' # set a property

    """
    def __init__(self, wavfile):
        """Load and parse a call recording.

        If there is a file with the same basename and extension of '.json'
        in the same diretory than it is expected to contain an object
        who's properties will be stored as key / values in this object

        Args:
            wavefile (str): Path to the wave file to load

        Raises:
            OSError: Unable to open wavfile
            ValueError: Unable to parse call log
        """
        io.FileIO.__init__(self, wavfile, 'rb')

        ts = datetime.datetime.fromtimestamp(int(os.path.getctime(wavfile)))
        ts = pytz.timezone(str(get_localzone())).localize(ts)
        ts = ts.astimezone(pytz.utc)

        self.data = {
            "created": ts
        }

        basename, _ = os.path.splitext(wavfile)

        self.key = os.path.basename(basename)

        # if trunk reecorder call log, exists, load it as properties
        try:
            with open(basename+".json") as fh:
                self.data.update(json.load(fh))
        except OSError:
            pass

    def __del__(self):
        self.close()


class Elasticsearch(object):
    """Store a call in elastic search"""

    doc_type = 'call'

    def __init__(self, hosts, index_pattern="trunk-indexer-%Y.%m.%d"):
        """Connect to elasticsearch

        Args:
            hosts: (see elasticsearch.Elasticsearch) passed directly
            index_pattern: The index pattern to use wheen storing documents.
                This will be passed to strftime() with the creation date
                of the call passedto put

        Raises:
            RuntimeError: Could not connect to elasticseatch
        """

        try:
            self.es = elasticsearch.Elasticsearch(hosts)
            self.es.info()
        except elasticsearch.ElasticsearchException as exc:
            raise RuntimeError(
                "Cannot connect to elasticsearch: {}".format(exc)
            )

        self.index_pattern = index_pattern

    def put(self, call):
        """Store a call

        Args:
            call (Call): The call to store

        Returns:
`           dict: The inserted document

        Raises:
            RuntimeError: Unable to store call
        """

        # ensure any geo points get indexed as such by es
        mappings = {
            "mappings": {
                "call": {
                    "properties": {
                        "location": {
                            "type": "geo_point"
                        }
                    }
                }
            }
        }

        index = call['created'].strftime(self.index_pattern)
        try:
            self.es.indices.create(index=index, body=mappings)
        except elasticsearch.ElasticsearchException as exc:
            if (exc.info['error']['type'] ==
                    'resource_already_exists_exception'):
                    pass
            else:
                raise RuntimeError(exc)

        try:
            return self.es.index(index, self.doc_type, call.data, call.key)
        except elasticsearch.ElasticsearchException as exc:
            raise RuntimeError(exc)
