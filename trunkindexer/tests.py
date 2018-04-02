from unittest.mock import patch
import os
import shutil
import tempfile
import unittest

from elasticsearch import ElasticsearchException

from trunkindexer.gis import GIS, Street
from trunkindexer.storage import Call, Elasticsearch

GIS_FIXTURES = os.path.join(os.path.dirname(__file__), 'fixtures', 'gis')
CALL_FIXTURES = os.path.join(os.path.dirname(__file__), 'fixtures', 'calls')


@patch('trunkindexer.storage.elasticsearch.Elasticsearch')
class TestElastic(unittest.TestCase):

    def test_elastic_bad(self, es):
        """RuntimeError is raised if elastic cannot be reached"""

        es.side_effect = ElasticsearchException('Lol')

        with self.assertRaises(RuntimeError):
            Elasticsearch(['foo'])

    def test_elastic_bad_put(self, es):
        """RuntimeError is raised if elastic cannot store a Document"""

        es().index.side_effect = ElasticsearchException('Lol')

        e = Elasticsearch(['foo'])

        with self.assertRaises(RuntimeError):
            e.put(Call(os.path.join(CALL_FIXTURES, 'sample.wav')))

    def test_elastic_good_put(self, es):
        """Valid Documents are stored"""

        es().indices.side_effect = ElasticsearchException('Lol')

        e = Elasticsearch(['foo'])
        c = Call(os.path.join(CALL_FIXTURES, 'sample.wav'))
        e.put(c)

        es().index.assert_called_with(
            c['created'].strftime(e.index_pattern),
            e.doc_type,
            c.data,
            c.key
        )


class TestCall(unittest.TestCase):
    def test_no_wavfile(self):
        """OSError is raised on invalid wav file"""
        with self.assertRaises(OSError):
            Call('/this/does/not/exist/file.wav')

    def test_bad_json(self):
        """ValueError is raised if an invalid callog exists"""
        with self.assertRaises(ValueError):
            Call(os.path.join(CALL_FIXTURES, 'bad.wav'))

    def test_malformed_json(self):
        """ValueError is raised if call log isn't a dict"""
        with self.assertRaises(ValueError):
            Call(os.path.join(CALL_FIXTURES, 'mal.wav'))

    def test_good_no_log(self):
        """Only the default fields are present when there is no callog"""
        c = Call(os.path.join(CALL_FIXTURES, 'nolog.wav'))

        self.assertEqual(len(c), 1)
        self.assertEqual(list(c.keys()), ['created'])

    def test_good_log(self):
        """When a call has a log file it is loaded"""
        c = Call(os.path.join(CALL_FIXTURES, 'sample.wav'))

        self.assertEqual(c['talkgroup'], 2105)

    def test_read(self):
        """Calling read returns wav data"""
        fn = os.path.join(CALL_FIXTURES, 'sample.wav')
        with open(fn, 'rb') as sauce:
            c = Call(fn)
            self.assertEqual(c.read(), sauce.read())


class TestGIS(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        self.gis = GIS(self.tempdir)

    def tearDown(self):
        shutil.rmtree(self.tempdir, ignore_errors=True)

    def test_bad_datadir(self):
        """OSError is raised when a bad datadir is passed"""
        with self.assertRaises(OSError):
            GIS('/dev/null/lol/not/a/dir')

    def test_bad_no_file(self):
        """OSError is rasied when the gis file doesn't exist"""
        with self.assertRaises(OSError):
            self.gis.load('/dev/null/not/a/file')

    def test_bad_invalid_format(self):
        """ValueError is rasied when the gis file is invalid"""
        with self.assertRaises(ValueError):
            self.gis.load('/dev/null')

    def test_malformed_geometry(self):
        """ValueError is raised when file does not contain MultilineStrings"""
        with self.assertRaises(ValueError):
            self.gis.load(
                os.path.join(GIS_FIXTURES, 'singlepoint/singlepoint.shp')
            )

    def test_malformed_props(self):
        """ValueError is raised when file does not contain MultilineStrings"""
        with self.assertRaises(ValueError):
            self.gis.load(os.path.join(GIS_FIXTURES, 'lol.geojson'))

    def test_load_good_default_props(self):
        """The correct number of streets and features is returned when
        a valid file is passed
        """
        nums, numf = self.gis.load(
           os.path.join(GIS_FIXTURES, 'sample.geojson')
        )
        self.assertEqual(nums, 3)
        self.assertEqual(numf, 209)

    def test_load_street_not_exist(self):
        """None is returned for non-existent streets"""
        self.gis.load(os.path.join(GIS_FIXTURES, 'sample.geojson'))
        self.assertIs(self.gis.street('NOT A THING'), None)


class TestStreet(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        self.gis = GIS(self.tempdir)
        self.gis.load(os.path.join(GIS_FIXTURES, 'sample.geojson'))

    def tearDown(self):
        shutil.rmtree(self.tempdir, ignore_errors=True)

    def test_load_street_invalid(self):
        """ValueError is raised if a street file is invalid"""
        with self.assertRaises(ValueError):
            Street('/dev/null')

    def test_load_street_good(self):
        """Street data is loaded from cache successfully"""
        u = self.gis.street('university')
        self.assertEqual('UNIVERSITY', u.name)
        self.assertEqual(repr(u), '<Street: UNIVERSITY>')
        self.assertEqual(u.from_even, 'fromr')
        self.assertEqual(len(u.features), 79)

        s = self.gis.street('sacramento')
        self.assertEqual('SACRAMENTO', s.name)
        self.assertEqual(repr(s), '<Street: SACRAMENTO>')
        self.assertEqual(s.from_even, 'fromr')
        self.assertEqual(len(s.features), 43)

        a = self.gis.street('ASHBY')
        self.assertEqual('ASHBY', a.name)
        self.assertEqual(repr(a), '<Street: ASHBY>')
        # fixture data has this street addresses flipped
        self.assertEqual(a.from_even, 'froml')
        self.assertEqual(len(a.features), 50)

    def test_address_to_loc_bad(self):
        """None is returned when an address isn't on the street"""
        u = self.gis.street('university')
        self.assertIs(u.number_to_location(31337), None)

    def test_address_to_loc_good(self):
        """lat, long is returned when an address is on the street"""
        u = self.gis.street('university')
        p = u.number_to_location(2000)

        # floating point, yay
        self.assertAlmostEqual(p.x, -122.270671937423)
        self.assertAlmostEqual(p.y, 37.87185444581831)

        # works for both sides of the street
        a = self.gis.street('ashby')
        p = a.number_to_location(2000)
        self.assertAlmostEqual(p.x, -122.269019172256)
        self.assertAlmostEqual(p.y, 37.85472898728935)

    def test_no_intersect(self):
        """None is returned if two streets don't intersect"""
        u = self.gis.street('university')
        a = self.gis.street('ashby')

        self.assertIs(u.intersection(a), None)

    def test_intersect(self):
        """lat, long is returned when streets intersect"""
        u = self.gis.street('university')
        s = self.gis.street('sacramento')

        p = u.intersection(s)

        self.assertAlmostEqual(p.x, -122.2819379842289)
        self.assertAlmostEqual(p.y, 37.87041056265982)

    def test_stt_name(self):
        cases = {
            '51st': 'FIFTY FIRST',
            '52nd': 'FIFTY SECOND',
            '53rd': 'FIFTY THIRD',
            '54th': 'FIFTY FOURTH',
            '55th': 'FIFTY FIFTH',
            '56th': 'FIFTY SIXTH',
            '57th': 'FIFTY SEVENTH',
            '58th': 'FIFTY EIGHTH',
            '59th': 'FIFTY NINETH',
            '60th': 'SIXTIETH',
            '61st': 'SIXTY FIRST',
            '64th': 'SIXTY FOURTH',
            'SIXTY-FIFTH': 'SIXTY FIFTH'
        }
        for inname, outname in cases.items():
            self.assertEqual(outname, self.gis._stt_street_name(inname))

    def test_streets(self):
        """A list of streets is returned when calling streets()"""
        self.assertEqual(
            ['ASHBY', 'SACRAMENTO', 'UNIVERSITY'],
            sorted(self.gis.streets())
        )
