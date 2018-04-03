import datetime
import os
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch, Mock

from elasticsearch import ElasticsearchException

from trunkindexer import cli
from trunkindexer.gis import GIS, Street
from trunkindexer.storage import Call, Elasticsearch, load_talkgroups
from trunkindexer.stt import Parser

GIS_FIXTURES = os.path.join(os.path.dirname(__file__), 'fixtures', 'gis')
CALL_FIXTURES = os.path.join(os.path.dirname(__file__), 'fixtures', 'calls')
TG_FIXTURES = os.path.join(os.path.dirname(__file__), 'fixtures', 'talkgroups')


class TestCLI(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        self.gis = GIS(self.tempdir)
        self.gis.load(os.path.join(GIS_FIXTURES, 'sample.geojson'))

    def tearDown(self):
        shutil.rmtree(self.tempdir, ignore_errors=True)

    def test_main_unknown_command(self):
        """An error is raised if an unknown command is provided"""

        fakeparser = Mock()
        fakeparser.parse_args().command = 'lol'

        cli.main(fakeparser)

        fakeparser.error.assert_called_once()

    @patch('trunkindexer.cli.load')
    @patch('trunkindexer.cli.index')
    def test_main_known_command(self, imock, lmock):
        """The corresponding function is called for valid commands"""

        cli.main(cli.make_parser(), ['load', '/path/to/gis'])
        lmock.assert_called_once()
        imock.assert_not_called()

        lmock.reset_mock()

        cli.main(cli.make_parser(), ['index', '/path/to/gis'])
        imock.assert_called_once()
        lmock.assert_not_called()

    @patch('trunkindexer.cli.load')
    def test_main_exceptions(self, lmock):
        """If the function raises known exceptions, they are handled"""

        lmock.side_effect = ValueError('bad news')

        fakeparser = Mock()
        fakeparser.parse_args().command = 'load'

        cli.main(fakeparser)
        fakeparser.error.assert_called_with('Unable to load: bad news')

        lmock.side_effect = OSError('bad news')
        cli.main(fakeparser)
        fakeparser.error.assert_called_with('Unable to load: bad news')

        lmock.side_effect = RuntimeError('a thing')
        cli.main(fakeparser)
        fakeparser.error.assert_called_with(lmock.side_effect)

        with self.assertRaises(KeyError):
            lmock.side_effect = KeyError()
            cli.main(fakeparser)

    @patch('trunkindexer.cli.load_talkgroups')
    @patch('trunkindexer.cli.GIS')
    def test_load_func_no_tg(self, gmock, tmock):
        """Load is called with the expected parameters"""
        gmock().load.return_value = (6, 9)

        parser = cli.make_parser()
        args = parser.parse_args(['-d', self.tempdir, 'load', '/a/gis.file'])
        cli.load(args)

        gmock.assert_called_with(self.tempdir)
        gmock().load.assert_called_with(
            args.gisfile,
            args.street_name,
            args.fromr,
            args.tor,
            args.froml,
            args.tol
        )

        tmock.assert_not_called()

    @patch('trunkindexer.cli.load_talkgroups')
    @patch('trunkindexer.cli.GIS')
    def test_load_func_tg(self, gmock, tmock):
        """Load is called with the expected parameters with talkgroups"""
        gmock().load.return_value = (6, 9)
        tmock.return_value = (6, 9)

        parser = cli.make_parser()
        args = parser.parse_args(
            ['-d', self.tempdir, 'load', '/a/gis.file', '/a/tg.csv']
        )
        cli.load(args)

        gmock.assert_called_with(self.tempdir)
        tmock.assert_called_with(self.tempdir, '/a/tg.csv')

    @patch('trunkindexer.cli.Elasticsearch')
    def test_index_func_no_transcript(self, emock):
        """Index is called with the expected paramters with no transcript"""
        parser = cli.make_parser()
        args = parser.parse_args([
            '-d',
            self.tempdir,
            'index',
            os.path.join(CALL_FIXTURES, 'sample.wav')
        ])
        c = cli.index(args)

        emock().put.assert_called_once()
        self.assertNotIn('transcript', c)
        self.assertNotIn('location', c)
        self.assertNotIn('detected_address', c)

    @patch('trunkindexer.cli.Elasticsearch')
    def test_index_func_transcript_good(self, emock):
        """Index is called with the expected paramters with a transcript"""
        parser = cli.make_parser()
        args = parser.parse_args([
            '-d',
            self.tempdir,
            'index',
            os.path.join(CALL_FIXTURES, 'sample.wav'),
            '--transcript',
            'nineteen ninety nine university'
        ])
        c = cli.index(args)

        self.assertTrue(emock().put.call_count, 2)
        self.assertIn('transcript', c)
        self.assertIn('location', c)
        self.assertEqual(c['detected_address'], '1999 UNIVERSITY')

    @patch('trunkindexer.cli.Elasticsearch')
    def test_index_func_transcript_bad(self, emock):
        """Index is called with the expected paramters with a transcript"""
        parser = cli.make_parser()
        args = parser.parse_args([
            '-d',
            self.tempdir,
            'index',
            os.path.join(CALL_FIXTURES, 'sample.wav'),
            '--transcript',
            'no address here'
        ])
        c = cli.index(args)

        self.assertTrue(emock().put.call_count, 2)
        self.assertIn('transcript', c)
        self.assertNotIn('location', c)
        self.assertNotIn('detected_address', c)


@patch('trunkindexer.storage.elasticsearch.Elasticsearch')
class TestElastic(unittest.TestCase):

    def test_bad(self, es):
        """RuntimeError is raised if elastic cannot be reached"""

        es.side_effect = ElasticsearchException('Lol')

        with self.assertRaises(RuntimeError):
            Elasticsearch(['foo'])

    def test_bad_put(self, es):
        """RuntimeError is raised if elastic cannot store a Document"""

        es().index.side_effect = ElasticsearchException('Lol')

        e = Elasticsearch(['foo'])

        with self.assertRaises(RuntimeError):
            e.put(Call(os.path.join(CALL_FIXTURES, 'sample.wav')))

    def test_dupe_index_put(self, es):
        """duplicate index errors are ignored"""
        exc = ElasticsearchException('dupe index')
        exc.info = {
            'error': {'type': 'resource_already_exists_exception'}
        }

        es().indices.create.side_effect = exc

        e = Elasticsearch(['foo'])

        # no exception should be rasied
        e.put(Call(os.path.join(CALL_FIXTURES, 'sample.wav')))

    def test_index_create_fail(self, es):
        """other index errors are caught"""
        exc = ElasticsearchException('other index error')
        exc.info = {
            'error': {'type': 'other_error'}
        }

        es().indices.create.side_effect = exc

        e = Elasticsearch(['foo'])

        with self.assertRaises(RuntimeError):
            e.put(Call(os.path.join(CALL_FIXTURES, 'sample.wav')))

    def test_good_put(self, es):
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

        self.assertEqual(len(c), 2)
        self.assertEqual(list(c.keys()), ['created', 'url'])

    def test_good_log(self):
        """When a call has a log file it is loaded"""
        c = Call(os.path.join(CALL_FIXTURES, 'sample.wav'))

        self.assertEqual(c['talkgroup'], 2105)

        # these fields are generated from the call log
        self.assertEqual(c['duration'], 10)
        self.assertTrue(isinstance(c['start_time'], datetime.datetime))

        # path is wrong, so no system extracting
        self.assertNotIn('system', c)
        self.assertEqual(c['talkgroup'], 2105)

    def test_read(self):
        """Calling read returns wav data"""
        fn = os.path.join(CALL_FIXTURES, 'sample.wav')
        with open(fn, 'rb') as sauce:
            c = Call(fn)
            self.assertEqual(c.read(), sauce.read())

    def test_tr_dir_to_system(self):
        """If the file is in a trunk-recorder path, extract the shortname"""
        c = Call(os.path.join(CALL_FIXTURES, 'excellent/2012/6/9/sample.wav'))
        self.assertEqual(c['system'], 'excellent')

    def test_baseurl(self):
        """base url is used if the path is a trunk recorder path"""

        c = Call(os.path.join(CALL_FIXTURES, 'sample.wav'))

        self.assertEqual(
            c['url'],
            'file://'+str(Path(os.path.join(CALL_FIXTURES, 'sample.wav')))
        )

        c = Call(
            os.path.join(CALL_FIXTURES, 'sample.wav'),
            baseurl='http://this.is.ignored'
        )

        self.assertEqual(
            c['url'],
            'file://'+str(Path(os.path.join(CALL_FIXTURES, 'sample.wav')))
        )

        c = Call(
            os.path.join(CALL_FIXTURES, 'excellent/2012/6/9/sample.wav'),
            baseurl='http://this.is.used/'
        )

        self.assertEqual(
            c['url'],
            'http://this.is.used/excellent/2012/6/9/sample.wav'
        )


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
        "Names are covered to spoken english"
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


class TestTalkGroups(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tempdir, ignore_errors=True)

    def test_load_talkgroups_bad(self):
        """OSError is raised if a bad talk group file is loaded"""

        with self.assertRaises(OSError):
            load_talkgroups(self.tempdir, '/this/file/does/not/exist')

    def test_load_talkgroups_malformed(self):
        """ValueError is raised when the talkgroup csv is missing fields"""

        with self.assertRaises(ValueError):
            load_talkgroups(self.tempdir, os.path.join(TG_FIXTURES, 'bad.csv'))

    def test_load_talkgroups_good(self):
        """Talkgroup csv will be parsed and cached to disk"""
        r, c = load_talkgroups(
                self.tempdir,
                os.path.join(TG_FIXTURES, 'sample.csv')
            )
        self.assertEqual(r, 2)
        self.assertEqual(c, 7)

    def test_talkgroups_call(self):
        """Extended talkgroup info is available when datadir is provided"""
        load_talkgroups(
            self.tempdir,
            os.path.join(TG_FIXTURES, 'sample.csv')
        )

        c = Call(
            os.path.join(CALL_FIXTURES, 'sample.wav'),
            datadir=self.tempdir
        )

        self.assertEqual(c['talkgroup']['DEC'], 2105)
        self.assertEqual(c['talkgroup']['Description'], 'Fire Dispatch 1')

    def test_missing_talkgroups_ignored(self):
        """if we ahve a data dir, but no talkgroup file, that's cool"""
        c = Call(
            os.path.join(CALL_FIXTURES, 'sample.wav'),
            datadir=self.tempdir
        )
        self.assertEqual(c['talkgroup'], 2105)


class TestParser(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        gis = GIS(self.tempdir)
        gis.load(os.path.join(GIS_FIXTURES, 'sample.geojson'))

        self.parser = Parser(self.tempdir)

    def tearDown(self):
        shutil.rmtree(self.tempdir, ignore_errors=True)

    def test_address_formats(self):
        """A variety of address formats can be decoded"""
        supported = [
            ('one two three university', ['123 UNIVERSITY']),
            ('twenty six eighteen ashby', ['2618 ASHBY']),
            ('two hundred university', ['200 UNIVERSITY']),
            ('nine hundred ashby', ['900 ASHBY']),
            ('twenty two hundred ashby', ['2200 ASHBY']),
            ('three to oh for sacramento', ['3204 SACRAMENTO']),
            ('twenty twenty sacramento', ['2020 SACRAMENTO']),
            ('university and sacramento', ['UNIVERSITY/SACRAMENTO']),
            # adrdresses are scored higher than intersections
            (
                'university and sacramento thats fifteen hundred university',
                ['1500 UNIVERSITY', 'UNIVERSITY/SACRAMENTO']
            ),
            # all "maybe" digits shouldn't return an address
            ('to to to sacramento', []),
            # streets that don't cross, don't return
            ('university and ashby', []),

        ]

        for val, output in supported:
            with self.subTest(val):
                self.assertEqual(
                    [x.value for x in self.parser.locations(val)],
                    output
                )

    def test_replace(self):
        """replace() updates txt with the found location"""
        val = "yo dogg we at nineteen ninety nine university lets party"
        loc = self.parser.locations(val)[0]

        self.assertEqual(
            loc.replace(val),
            'yo dogg we at 1999 UNIVERSITY lets party'
        )
