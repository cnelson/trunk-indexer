import csv
import io
from collections import UserDict
import datetime
import json
import os
from pathlib import Path
import re

import elasticsearch

import pytz
from tzlocal import get_localzone

from trunkindexer import kaldi

TGPATH = 'talkgroups/talkgroups.json'


def load_talkgroups(datadir, tgfile):
    """Cache a talkgroups csv file

    Args:
        datadir (str): Where to cache the file
        tgfile (str): Path to a talkgroups CSV file
            It must have a header row with a column named DEC
            which is the talkgroup id in decimal

    Returns:
        (int, int): The number of records and fields cached

    Raises:
        ValueError: inappropriately formated csv
        OSError: Could not cache the file
    """
    tgs = {}
    with open(tgfile) as fh:
        reader = csv.DictReader(fh)

        try:
            for row in reader:
                row['DEC'] = int(row['DEC'])
                tgs[str(row['DEC'])] = row
        except (KeyError, ValueError):
            raise ValueError("DEC column is missing or not an int.")

    fn = os.path.join(datadir, TGPATH)
    os.makedirs(os.path.dirname(fn), exist_ok=True)

    with open(fn, 'w') as fh:
        json.dump(tgs, fh)

    return len(tgs), len(list(tgs.values())[0])


class Call(UserDict, io.FileIO):
    """An object that represents a record call and associated metadata

    c = Call('/tmp/some.wav')
    c.read() # returns wav data

    c['created'] # returns the created property

    c['some key'] = 'Some value' # set a property

    """
    def __init__(self, wavfile, baseurl=None, datadir=None):
        """Load and parse a call recording.

        If there is a file with the same basename and extension of '.json'
        in the same diretory than it is expected to contain an object
        who's properties will be stored as key / values in this object

        Args:
            wavefile (str): Path to the wave file to load
            baseurl (str): If provided, will override the base url
                used when generating links to media
            datadir (str): If providfed, will attempt to load talkgroups
                data from this directory

        Raises:
            OSError: Unable to open wavfile
            ValueError: Unable to parse call log
        """
        io.FileIO.__init__(self, wavfile, 'rb')

        ts = datetime.datetime.fromtimestamp(int(os.path.getctime(wavfile)))
        ts = pytz.timezone(str(get_localzone())).localize(ts)
        ts = ts.astimezone(pytz.utc)

        self.data = {
            "created": ts,
            "url": 'file://' + str(Path(wavfile).resolve())
        }

        basename, ext = os.path.splitext(wavfile)

        self.key = os.path.basename(basename)

        self.datadir = datadir

        # if this looks like a trunk recorder formatted path
        # grab the shortName out of the path
        m = re.search(
            r'(\w+)\/\d{4}\/\d+\/\d+\/'+re.escape(self.key)+'$',
            basename
        )
        if m is not None:
            self.data['system'] = m.groups()[0]

            if baseurl is not None:
                baseurl = baseurl.rstrip('/')
                self.data['url'] = baseurl + '/' + m.group() + ext

        # if trunk reecorder call log, exists, load it as properties
        # and format it for use in elasticsearch
        try:
            with open(basename+".json") as fh:
                self.data.update(json.load(fh))
                self.data['duration'] = (
                    self.data['stop_time'] -
                    self.data['start_time']
                )
                self.data['start_time'] = datetime.datetime.fromtimestamp(
                    self.data['start_time']
                )
                self.data['stop_time'] = datetime.datetime.fromtimestamp(
                    self.data['stop_time']
                )
        except OSError:
            pass

        # load extended talkgroups info, if we have a callog and
        # talkgroup information
        if 'talkgroup' in self.data and datadir:
            try:
                with open(
                    os.path.join(datadir, TGPATH)
                ) as fh:
                    tgs = json.load(fh)
                    self.data['talkgroup'] = tgs.get(
                        str(self.data['talkgroup']),
                        self.data['talkgroup']
                    )
            except OSError:
                # ignore missing talkgroups file
                pass

    def transcribe(self):
        """Use speech to text to transcribe a call

        Returns:
            str: The transcription

        Raises:
            RuntimeError: Could not transcribe audio
        """
        if self.datadir:
            self.data['transcript'] = kaldi.decode(
                    self.name,
                    os.path.join(self.datadir, 'stt')
            )

            return self.data['transcript']
        else:
            raise RuntimeError("datadir not configured")

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
