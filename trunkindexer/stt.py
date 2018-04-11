import collections
import glob
import gzip
import os
import pickle
import re
import shutil
from subprocess import Popen, PIPE

from lark import Lark, Transformer
from lark.lexer import UnexpectedInput
from lark.common import ParseError

from sequitur import Translator

from trunkindexer.gis import GIS

try:
    from trunkindexer._config import KALDI_HOME
except ImportError:
    KALDI_HOME = None

KALDI_HOME = os.environ.get('KALDI_HOME', KALDI_HOME)

BASE_GRAMMAR = """
%import common.WS
%ignore WS

location: cross | addr
cross: (street and street)
addr: (number+ street type?)

and: AND
number: DIGIT | NUMBER | MAYBE | PREFIX | SUFFIX
street: STREET
type: TYPE

AND: "and" | "at"
MAYBE: "oh" | "to" | "for" | "or"
DIGIT: "one" | "two" | "three" | "four" | "five" | "six" | "seven" | "eight" | "nine"
NUMBER: "zero" | "ten" | "eleven" | "tweleve" | "thirteen" | "fourteen" | "fifteen" | "sixteen" | "seventeen" | "eighteen" | "nineteen"
PREFIX: "twenty" | "thirty" | "forty" | "fourty" | "fifty" | "sixty" | "seventy" | "eighty" | "ninety"
SUFFIX: "hundred"
TYPE: "crossroads" | "expressway" | "extensions" | "throughway" | "trafficway" | "boulevard" | "crossroad" | "extension" | "junctions" | "mountains" | "stravenue" | "underpass" | "causeway" | "crescent" | "crossing" | "junction" | "motorway" | "mountain" | "overpass" | "parkways" | "turnpike" | "villages" | "centers" | "circles" | "commons" | "corners" | "estates" | "freeway" | "gardens" | "gateway" | "harbors" | "heights" | "highway" | "islands" | "landing" | "meadows" | "mission" | "orchard" | "parkway" | "passage" | "prairie" | "springs" | "squares" | "station" | "streets" | "terrace" | "trailer" | "valleys" | "viaduct" | "village" | "arcade" | "avenue" | "bluffs" | "bottom" | "branch" | "bridge" | "brooks" | "bypass" | "canyon" | "center" | "circle" | "cliffs" | "common" | "corner" | "course" | "courts" | "divide" | "drives" | "estate" | "fields" | "forest" | "forges" | "garden" | "greens" | "groves" | "harbor" | "hollow" | "island" | "knolls" | "lights" | "manors" | "meadow" | "plains" | "points" | "radial" | "rapids" | "ridges" | "shoals" | "shores" | "skyway" | "spring" | "square" | "stream" | "street" | "summit" | "tunnel" | "unions" | "valley" | "alley" | "bayou" | "beach" | "bluff" | "brook" | "burgs" | "cliff" | "court" | "coves" | "creek" | "crest" | "curve" | "drive" | "falls" | "ferry" | "field" | "flats" | "fords" | "forge" | "forks" | "glens" | "green" | "grove" | "haven" | "hills" | "inlet" | "knoll" | "lakes" | "light" | "locks" | "lodge" | "manor" | "mills" | "mount" | "parks" | "pines" | "place" | "plain" | "plaza" | "point" | "ports" | "ranch" | "rapid" | "ridge" | "river" | "roads" | "route" | "shoal" | "shore" | "spurs" | "trace" | "track" | "trail" | "union" | "views" | "ville" | "vista" | "walks" | "wells" | "anex" | "bend" | "burg" | "camp" | "cape" | "club" | "cove" | "dale" | "fall" | "flat" | "ford" | "fork" | "fort" | "glen" | "hill" | "isle" | "keys" | "lake" | "land" | "lane" | "loaf" | "lock" | "loop" | "mall" | "mews" | "mill" | "neck" | "oval" | "park" | "pass" | "path" | "pike" | "pine" | "port" | "ramp" | "rest" | "road" | "spur" | "view" | "walk" | "wall" | "ways" | "well" | "dam" | "key" | "row" | "rue" | "run" | "way"
"""  # noqa: E501

SPOKEN_TO_INT = {
    "oh": 0,
    "zero": 0,
    "one": 1,
    "to": 2,
    "two": 2,
    "three": 3,
    "for": 4,
    "four": 4,
    "five": 5,
    "six": 6,
    "seven": 7,
    "eight": 8,
    "nine": 9,
    "ten": 10,
    "eleven": 11,
    "twelve": 12,
    "thirteen": 13,
    "fourteen": 14,
    "fifteen": 15,
    "sixteen": 16,
    "seventeen": 17,
    "eighteen": 18,
    "nineteen": 19,
    "twenty": 20,
    "thirty": 30,
    "forty": 40,
    "fourty": 40,
    "fifty": 50,
    "sixty": 60,
    "seventy": 70,
    "eighty": 80,
    "ninety": 90,
    "hundred": 100
}


class LanguageModel(object):
    """Wrapper around kaldi tools for building a language model

    Based on:
        http://kaldi-asr.org/doc/online_decoding.html#online_decoding_nnet2
        https://chrisearch.wordpress.com/2017/03/11/speech-recognition-using-
        kaldi-extending-and-using-the-aspire-model/
    """

    def __init__(self, datadir):
        """
            Args:
            datadir (str): The path to store the model
        """
        self.datadir = datadir
        self.sttdir = os.path.join(self.datadir, 'stt')

        os.makedirs(self.sttdir, exist_ok=True)

        self.sourcedir = os.path.join(
            os.path.dirname(__file__),
            'data/aspire'
        )

        self.lmplz = os.path.join(
            os.path.dirname(__file__),
            'vendor/kenlm/bin/lmplz'
        )

    def update(self, corpus):
        """Compile a new langauge model with additional words

        Args:
            corpus list[str, str...]: A list of phrases to add to the model

        """
        # copy the aspire model to the data directory
        files = [
            'final.dubm',
            'final.ie',
            'final.mat',
            'global_cmvn.stats',
            'extra_questions.txt',
            'nonsilence_phones.txt',
            'optional_silence.txt',
            'silence_phones.txt',
            'phones.txt'
        ]

        for fn in files:
            shutil.copyfile(
                os.path.join(self.sourcedir, fn),
                os.path.join(self.sttdir, fn)
            )

        # broken up into < 100 MB chunks so github will host
        with open(os.path.join(self.sttdir, 'final.mdl'), 'wb') as fh:
            for fn in glob.glob(os.path.join(self.sourcedir, 'final.mdl-*')):
                with open(fn, 'rb') as tmp:
                    fh.write(tmp.read())

        with open(os.path.join(self.sourcedir, 'cmudict-model'), 'rb') as fh:
            model = pickle.load(fh, encoding='latin1')

        translator = Translator(model)
        del model

        # guess pronouncitions
        with open(os.path.join(self.sttdir, 'words.dic'), 'w') as fh:
            for word in corpus:
                word = word.upper()
                left = tuple(word)
                try:
                    result = translator(left)
                    fh.write("{}\t{}\n".format(word, ' '.join(result)))
                except RuntimeError:
                    print("Couldn't translate {}".format(word))

        # trigram language model of street names
        with open(os.path.join(self.sttdir, 'lm.arpa'), 'wb') as fh:
            p = Popen([self.lmplz, '-o', '3'], stdout=PIPE, stdin=PIPE)
            stdout, stderr = p.communicate("\n".join(corpus).encode('utf-8'))
            fh.write(stdout)
            p.wait()

        words = collections.OrderedDict()
        for dic in [
            os.path.join(self.sourcedir, 'lexicon4_extra.txt'),
            os.path.join(self.sttdir, 'words.dic')
        ]:
            with open(dic, 'r+') as fh:
                for line in fh:
                    arr = line.strip().replace("\t", " ").split(" ", 1)
                    [word, pronunciation] = arr
                    word = word.lower()
                    if word not in words:
                        words[word] = set([pronunciation.lower()])
                    else:
                        words[word].add(pronunciation.lower())

        # jank merge of our dict / lm
        # with original model
        with open(os.path.join(self.sttdir, 'lexicon.txt'), 'w') as fh:
            for word in words:
                for pronunciation in words[word]:
                    fh.write(word + " " + pronunciation + "\n")

        grams = [[], [], []]
        for lm in [
            os.path.join(self.sourcedir, 'lm_unpruned'),
            os.path.join(self.sttdir, 'lm.arpa')
        ]:
            with open(lm, 'r+') as fh:
                mode = 0
                for line in fh:
                    line = line.strip()
                    if line == "\\1-grams:":
                        mode = 1
                    if line == "\\2-grams:":
                        mode = 2
                    if line == "\\3-grams:":
                        mode = 3
                    arr = line.split(" ")
                    if mode > 0 and len(arr) > 1:
                        if mode == 1 or mode == 2:
                            word = " ".join(arr[2:-1] if mode < 3 else arr[2:])
                            word = word.lower()
                        grams[mode - 1].append(line.lower())

        with gzip.open(
            os.path.join(self.sttdir, 'merged-lm.arpa.gz'),
            'wb'
        ) as fh:
            fh.write(
                bytes(
                    u"\\data\\\n" +
                    u"ngram 1=" + str(len(grams[0])) + "\n"
                    u"ngram 2=" + str(len(grams[1])) + "\n"
                    u"ngram 3=" + str(len(grams[2])) + "\n",
                    'utf-8'
                )
            )
            for i in range(3):
                fh.write(bytes(u"\n\\" + str(i+1) + u"-grams:\n", 'utf-8'))
                for g in grams[i]:
                    fh.write(bytes(g + "\n", 'utf-8'))

            fh.write(bytes(u"\n\\end\\\n", 'utf-8'))

        aspire = os.path.join(KALDI_HOME, 'egs/aspire/s5')
        p = Popen(
            [
                'utils/prepare_lang.sh',
                '--phone-symbol-table',
                'exp/tdnn_7b_chain_online/phones.txt',
                self.sttdir,
                "<unk>",
                '/tmp',
                self.sttdir
            ],
            cwd=aspire
        )
        p.wait()

        p = Popen(
            [
                'utils/format_lm.sh',
                self.sttdir,
                os.path.join(self.sttdir, 'merged-lm.arpa.gz'),
                os.path.join(self.sttdir, 'lexicon.txt'), self.sttdir
            ],
            cwd=aspire
        )
        p.wait()

        p = Popen(
            [
                'utils/mkgraph.sh',
                '--self-loop-scale',
                '1.0',
                self.sttdir,
                'exp/tdnn_7b_chain_online',
                self.sttdir
            ],
            cwd=aspire
        )


class Address(Transformer):
    """Convert a spoken address into one suitable for searching GIS data"""

    def location(self, args):
        return args

    def addr(self, args):
        """Transform the numbers in a street address to int

        twenty nine sixteen => 2916
        """
        numbers = []

        street_number = []
        street_name = []

        # split the address into name and numbers
        for c in args:
            try:
                if c.data == 'number':
                    numbers.append(c.children[0])
            except AttributeError:
                street_name.append(c)

        # if we only have maybe numbers, then give up
        maybes = 0
        for num in numbers:
            if num.type == "MAYBE":
                num.type = 'DIGIT'
                maybes += 1

        if maybes == len(numbers) and maybes:
            return None

        # walk through our numbers, and look around to calculate
        # the real value of each token
        for i, num in enumerate(numbers):
            val = SPOKEN_TO_INT[num.value]

            # stand alone numbers like 'thirteen'
            if num.type == "NUMBER":
                try:
                    ahead = numbers[i+1]
                    if ahead.type == "SUFFIX":
                        val = val * SPOKEN_TO_INT[ahead.value]
                except IndexError:
                    pass

            # numbers this might be added to a digit,
            # like 'twenty' in 'twenty two' -> 22
            # or could just be standalone 'fourty fourty' -> 4040
            elif num.type == "PREFIX":
                # if the next token is a digit, then add it to our value
                try:
                    ahead = numbers[i+1]
                    if ahead.type == "DIGIT":
                        val += SPOKEN_TO_INT[ahead.value]

                    ahead = numbers[i+2]
                    if ahead.type == "SUFFIX":
                        val = val * SPOKEN_TO_INT[ahead.value]
                except IndexError:
                    pass

            # 1-9
            elif num.type == "DIGIT":
                # if the number before is a prefix, then we already
                # "used" this digit above
                if i > 0 and numbers[i-1].type == "PREFIX":
                        continue

                # if the number after is a suffix, and there's no prefix
                # before then multiple.  ex: "three hundred"
                try:
                    ahead = numbers[i+1]
                    if ahead.type == "SUFFIX":
                        val = val * SPOKEN_TO_INT[ahead.value]
                except IndexError:
                    pass

            # 'hundred'
            elif num.type == "SUFFIX":
                # if we are a suffix, but there's no digit before us
                # then return unmodified
                if i > 0 and numbers[i-1].type in ["NUMBER", "DIGIT"]:
                    continue

            street_number.append(val)

        street_number = int(''.join([str(x) for x in street_number]))
        street_name = ' '.join(street_name)
        return 'addr', street_number, street_name

    def cross(self, args):
        return 'cross', args[0], args[2]

    def street(self, args):
        return args[0].value.upper()


class Location (object):
    """Represents a location extracted from search results"""

    def __init__(self, value, point, base_score=0):
        """Args:
            value (str): The text associated with a location: '123 MAIN'
            point (shapely.Point): The detected location
            base_score (int): The base score of this match
        """
        self.value = value
        self.point = point
        self._score = base_score
        self.positions = []

    def replace(self, txt):
        """Given the source txt this location was extracted from
        return the txt with the original text replaced with our transformed
        version

        Args:
            txt (str): The text this location object was created from
        """

        # keep the original text length, so other positions aren't off
        for start, end in self.positions:
            txt = txt[0:start] + '~' * (end-start) + txt[end:]

        # replace our placeholders with the value all at once
        txt = re.sub(r'\~+', self.value, txt)

        return txt

    def score(self):
        """Return a score based on the base score + number of matches

        Returns:
            int: score
        """
        return self._score + len(self.positions)

    def add_postion(self, start, end):
        """Add a position in the original text where this location was found
        Multiple positions can be added, if the same location appears in the
        text more that once

        Args:
            start (int): The start position
            end (int): The end poistion

            start and end should be provided so that
            transcript[start:end] would return the matched text
        """
        self.positions.append((start, end))
        self.positions.sort()

    def __str__(self):
        return '<Location: {} ({}, {}); s:{}>'.format(
            self.value,
            self.point.y,
            self.point.x,
            self.score()
        )

    def __repr__(self):
        return self.__str__()


class Parser(object):
    """Parse a given block of text and extract meaningful info"""

    def __init__(self, datadir):
        """Create a parser.  If the grammar isn't already on disk
        it will be created

        Raises:
            OSError: Could not load grammar from disk
        """

        self.datadir = datadir
        self.larkdir = os.path.join(self.datadir, 'lark')
        self.gis = GIS(self.datadir)

        fn = os.path.join(self.larkdir, 'location.g')
        try:
            fh = open(fn)
        except OSError:
            self._build_grammar(fn)
            fh = open(fn)

        try:
            self._parser = Lark(
                fh.read(),
                start='location',
                ambiguity='explicit'
            )
        finally:
            fh.close()

    def _build_grammar(self, filename):
        """Create a grammar by combining the BASE_GRAMMAR
        with a list of streets

        Args:
            filename (str): Where to write the grammar

        Returns:
            True:  The grammar was written to disk

        Raises:
            OSError: Unable to write grammar to disk
        """

        os.makedirs(os.path.dirname(filename), exist_ok=True)

        with open(filename, 'w') as fh:
            fh.write(BASE_GRAMMAR)
            fh.write(
                'STREET: /' +
                '|'.join(sorted(
                    [x.lower() for x in self.gis.streets()],
                    key=len,
                    reverse=True
                )) + '/'
            )

        return True

    def _find_addr(self, transcript):
        """Return a single addr

        Args:
            transcript (str): The text to search

        Returns:
            None:  No location as was found
            (tree, match_start, match_end): The address, and start/end pos
        """

        words = transcript.split(' ')
        tree = None
        spos = None
        epos = None
        for start in range(len(words)):
            for end in range(start+1, len(words)+1):
                try:
                    chunk = " ".join(words[start:end])
                    tree = self._parser.parse(chunk)
                    spos = len("".join(words[0:start])+(" "*start))
                    epos = spos+len(chunk)
                except (ParseError, UnexpectedInput) as exc:
                    if tree:
                        break
                    else:
                        continue
            if tree:
                break

        return tree, spos, epos

    def locations(self, transcript):
        """Search transcript for locations

        Args:
            transcript (str): The text to search

        Returns:
            [Location, Location, ...]: The locations found.
            This list will be empty if no locations were found
        """

        trees = []
        locations = {}

        # find all non-overlapping locations
        pos = 0
        while pos < len(transcript):
            tree, spos, epos = self._find_addr(transcript[pos:])
            if tree is None:
                break

            trees.append([tree, pos+spos, pos+epos])

            pos = epos + pos + 1

        # for each matched pattern
        for tree, spos, epos in trees:
            loc = None
            score = 0

            # convert it into queryable
            addr = Address().transform(tree)[0]
            if addr is None:
                continue

            if addr[0] == 'addr':
                s = self.gis.street(addr[2])
                if s is not None:
                    val = '{} {}'.format(addr[1], addr[2])
                    # give valid street addresses a boost in scoring
                    score = 1
                    loc = s.number_to_location(addr[1])

            elif addr[0] == 'cross':
                s1 = self.gis.street(addr[1])
                s2 = self.gis.street(addr[2])

                if s1 is not None and s2 is not None:
                    val = '{}/{}'.format(addr[1], addr[2])
                    loc = s1.intersection(s2)

            # loc will be none if a locaton could not be found
            # this can happen if the address is out of bounds
            if loc is not None:
                # group matches together by their value
                try:
                    ll = locations[val]
                except KeyError:
                    locations[val] = Location(val, loc, score)
                    ll = locations[val]

                ll.add_postion(spos, epos)

        # return all matches sorted by score
        return sorted(
            locations.values(),
            key=lambda x: x.score(),
            reverse=True
        )
