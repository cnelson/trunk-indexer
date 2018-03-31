import os
import shutil
import tempfile
import unittest

from trunkindexer.gis import GIS, Street

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), 'fixtures')


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
                os.path.join(FIXTURES_DIR, 'singlepoint/singlepoint.shp')
            )

    def test_malformed_props(self):
        """ValueError is raised when file does not contain MultilineStrings"""
        with self.assertRaises(ValueError):
            self.gis.load(os.path.join(FIXTURES_DIR, 'lol.geojson'))

    def test_load_good_default_props(self):
        """The correct number of streets and features is returned when
        a valid file is passed
        """
        nums, numf = self.gis.load(
            os.path.join(FIXTURES_DIR, 'sample.geojson')
        )
        self.assertEqual(nums, 3)
        self.assertEqual(numf, 209)

    def test_load_street_not_exist(self):
        """None is returned for non-existent streets"""
        self.gis.load(os.path.join(FIXTURES_DIR, 'sample.geojson'))
        self.assertIs(self.gis.street('NOT A THING'), None)


class TestStreet(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        self.gis = GIS(self.tempdir)
        self.gis.load(os.path.join(FIXTURES_DIR, 'sample.geojson'))

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
