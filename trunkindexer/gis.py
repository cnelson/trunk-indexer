import os
import shutil

from shapely.geometry import shape, Point
from shapely.ops import linemerge
import fiona


class Street(object):
    """An object represemting a single street"""

    def __init__(self, filename):
        """Load the street data from disk

        Args:
            filename (str): The filename to load

        Raises:
            OSError: The file could not be opened
            ValueError: The file format was invalid
        """
        self.name = ""

        try:
            with fiona.drivers():
                self.features = fiona.open(filename, 'r')
        except fiona.errors.FionaValueError as exc:
            raise ValueError("Unable to load: {}".format(exc))

        self.name = self.features[0]['properties']['name']

        # figure out which side is odd/even
        if self.features[0]['properties']['fromr'] % 2 == 0:
            self.from_even = 'fromr'
            self.to_even = 'tor'
            self.from_odd = 'froml'
            self.to_odd = 'tol'
        else:
            self.from_even = 'froml'
            self.to_even = 'tol'
            self.from_odd = 'fromr'
            self.to_odd = 'tor'

    def __del__(self):
        """Ensure shapefiles are closed when we go away"""
        try:
            self.features.close()
        except AttributeError:
            pass

    def __repr__(self):
        return '<{}: {}>'.format(self.__class__.__name__, self.name)

    def number_to_location(self, number):
        """Estimate the lat, long for a given street number

        This estimation is based on an usually incorrect assumption that
        addresses are linearly spaced along a street but even when they are
        not this approximation is close enough for the app's needs

        Args:
            number (int): The street number

        Returns:
            Point: The estimated lat, lng
            None: The street number does not exist on this street
        """

        if number % 2 == 0:
            start = self.from_even
            end = self.to_even
        else:
            start = self.from_odd
            end = self.to_odd

        # find the segment that contains our address
        for feat in self.features:
            if (number >= feat['properties'][start]
                    and number <= feat['properties'][end]):

                    line = shape(feat['geometry'])

                    # calculate how "far down" the address is
                    pos = (
                        float(number - feat['properties'][start]) /
                        (feat['properties'][end] - feat['properties'][start])
                    )

                    # interpolate a new point that far down the line
                    point = line.interpolate(
                        Point(line.coords[0]).distance(
                            Point(line.coords[-1])
                        ) * pos
                    )

                    return point
        return None

    def intersection(self, other_street):
        """Returns the locatoin of where this street intersects with another

        Args: other_street (Street): The street to check

        Returns:
            Point: The lat, long of the intersection
            None: The streets do not intersect
        """

        # create one huge line for each of our streets
        us = linemerge([shape(x['geometry']) for x in self.features])
        them = linemerge(
            [shape(x['geometry']) for x in other_street.features]
        )

        # find the intersection
        result = us.intersection(them)
        if result.type == 'Point':
            return result

        # work around for messy source data, that sometimes returns
        # multiple false intersections
        for point in result:
            if us.distance(point) == 0 and them.distance(point) == 0:
                return point

        # no intersection
        return None


class GIS(object):
    """Convert a GIS Street Centerline file into a format suitable for this
    libraries uses.

    This library's lookups only require at most data on two streets.
    We don't want to parse an entire GIS file and then interate through
    it's features each time we need to load a street by name, and then
    performing expensive computations on the resulting data.

    In order to solve this the following tasks are peformed:
        Create an individual shape file for each street containing all it's
        features

        Create lookup tables for expensive calculations and store them along
        with the shape data (TODO)

    Addtionally, we format thed data to make it more suitable for querying by
    speech-to-text.  Removing characters from street names that out STT engine
    won't produce, and converting abbreviations (ln -> lane) to how they are
    spoken.

    """

    def __init__(self, datadir):
        """Configure the importer

        Args:
            datadir (str): The path where data will be stored.
                It will be created if it doesn't exist, and
                EXISTING CONTENT IN IT MAY BE OVERWRITTEN.

        Raises:
            OSError: Couldn't create datadir
        """

        # will raise OSError if issue
        os.makedirs(datadir, exist_ok=True)

        self.datadir = datadir
        self.gisdir = os.path.join(datadir, 'gis')

    def load(
        self,
        gisfile,
        streetname="name",
        fromr="fromr",
        tor="tor",
        froml="froml",
        tol="tol"
    ):
        """Load a fiona-supported gis file and do the needful

        Args:
            gisfile (str): A path to the GIS file
            streetname (str): The name of the attribute containing the
                street name without suffixes "MAIN" not "MAIN ST"
            fromr (str): The name of the attribute constaining the start
                of right hand addresses
            tor (str): The name of the attribute constaining the end
                of right hand addresses
            from (str): fromr for the left side
            tol (str): tor for the left side

        Raises:
            OSError: A file system error occured
            ValueError: The gisfile provided could not be parsed.

        Returns:
            (int, int) - The number of streets and features loaded
        """

        shutil.rmtree(self.gisdir, ignore_errors=True)
        os.makedirs(self.gisdir)

        num_streets = 0
        num_features = 0
        with fiona.drivers():
            try:
                source = fiona.open(gisfile)
            except fiona.errors.FionaValueError as exc:
                raise ValueError("Unable to load: {}".format(exc))

            if source.meta['schema']['geometry'] != 'MultiLineString':
                raise ValueError(
                    "Unable to load file. Expected MultiLineString"
                    " geometry, got: {}".format(
                        source.meta['schema']['geometry']
                    )
                )

            # make sure all the attribute names exist
            for attr in (streetname, fromr, tor, froml, tol,):
                if attr not in source.meta['schema']['properties']:
                    raise ValueError(
                        "Attribute {} not found in {}".format(
                            attr,
                            gisfile
                        )

                    )

            # replace their properties with only the ones we care about
            # and name them consistently so we can find them later
            target_schema = source.schema.copy()
            target_schema['properties'] = {
                'name':  'str',
                'fromr': 'int',
                'tor':   'int',
                'froml': 'int',
                'tol':   'int'
            }

            cur = None
            fh = None
            for feat in source:
                num_features += 1
                # skip features with no street name
                if (feat['properties'][streetname] is None or
                        feat['properties'][streetname] == ""):
                    continue

                sttname = self._stt_street_name(
                    feat['properties'][streetname]
                )

                # if the record isn't for the file we have open
                # then close our current file and open the correct one
                if cur != sttname:
                    cur = sttname

                    if fh is not None:
                        fh.close()

                    fn = self._street_filename(cur)

                    # if the file doesn't already exist, create it
                    try:
                        fh = fiona.open(fn, 'a')
                    except OSError:
                        os.makedirs(os.path.dirname(fn), exist_ok=True)
                        fh = fiona.open(
                            fn,
                            'w',
                            crs=source.crs,
                            schema=target_schema,
                            driver='ESRI Shapefile'
                        )
                        num_streets += 1

                # map the source format, to our desired format
                feat['properties'] = {
                    'name': sttname,
                    'fromr': feat['properties'][fromr],
                    'tor': feat['properties'][tor],
                    'froml': feat['properties'][froml],
                    'tol': feat['properties'][tol],
                }

                fh.write(feat)

        fh.close()
        source.close()
        return num_streets, num_features

    def _street_filename(self, name):
        """Returns the filename for a given street file.

        This file is not guaranteed to exist.

        Args:
            name (str): The name of the street

        Returns:
            str: The filename
        """
        name = name.upper()

        return os.path.join(
            self.gisdir,
            name[0],
            name,
            name+'.shp'
        )

    def _stt_street_name(self, name):
        """Convert common gis representations of stret names to how
        a human would speak them

        TOOD: handle many other cases '65th' -> 'sixty fith', '3rd' -> 'third'

        Examples:
            sixty-fith -> "sixty fifth"
            "9th" -> "ninth"
            "63rd" -> "sixty third"
        """
        name = name.strip()
        name = name.replace('-', ' ')
        name = name.replace('/', ' ')

        return name.upper()

    def street(self, name):
        """Return a Street object for a given street name

        Args:
            name (str): The name of the street

        Returns:
            None: No street by this name
            Street: The street object

        Raises:
            ValueError: The street data is corrupt or invalid.
        """

        try:
            return Street(self._street_filename(name))
        except OSError as exc:
            return None
