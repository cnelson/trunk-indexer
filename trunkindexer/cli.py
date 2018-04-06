import argparse
import sys
import time

from colorama import init, Fore, Style

from trunkindexer.gis import GIS
from trunkindexer.storage import Elasticsearch, Call, load_talkgroups
from trunkindexer.stt import Parser


def make_parser():
    """Create a configured argparser

    Returns:
        argparse.ArgumentParser
    """

    parser = argparse.ArgumentParser(
        description="put trunk-recorder data in elasticsearch",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        '-d',
        '--data-dir',
        help="Data directory",
        default='.cachemoney'
    )
    parser.add_argument(
        '-e',
        '--elasticsearch',
        help="Where is elasticsearch",
        default="localhost:9200"
    )
    subparsers = parser.add_subparsers(dest='command')
    subparsers.required = True
    load = subparsers.add_parser(
        'load',
        help="Convert GIS data to geocoding lookup + grammar"
    )
    load.add_argument(
        '--street-name',
        default='street_nam',
        help="The property with the street name without suffixes."
    )
    load.add_argument(
        '--fromr',
        default='fromr',
        help="The property with the start address for the right side"
    )
    load.add_argument(
        '--tor',
        default='tor',
        help="The property with the end address for the right side"
    )
    load.add_argument(
        '--froml',
        default='froml',
        help="The property with the start address for the left side"
    )
    load.add_argument(
        '--tol',
        default='tol',
        help="The property with the end address for the left side"
    )
    load.add_argument(
        'gisfile',
        help="Street Centerline GIS data in geojson format",
    )
    load.add_argument(
        'tgfile',
        help="Talkgroups CSV file. Must have a header row, and a column"
             " named DEC",
        nargs='?',
        default=None
    )

    index = subparsers.add_parser(
        'index',
        help="Add call to elasticseatrch"
    )
    index.add_argument(
        'wavfile',
        help="Path to recording"
    )
    index.add_argument(
        '--baseurl',
        help="The base url where this wav file is available",
        default=None
    )
    index.add_argument(
        '--transcribe',
        default=False,
        action='store_true',
        help='Use STT to transcribe the recording'
    )

    return parser


def load(args):
    """Load geojson and talkgroups data into a datadir"""
    gis = GIS(args.data_dir)

    t = time.process_time()
    num_streets, num_features = gis.load(
        args.gisfile,
        args.street_name,
        args.fromr,
        args.tor,
        args.froml,
        args.tol
    )
    elapsed_time = time.process_time() - t

    print(
        Style.RESET_ALL + "Loaded "
        + Fore.GREEN + str(num_streets)
        + Style.RESET_ALL + " streets / "
        + Fore.GREEN + str(num_features)
        + Style.RESET_ALL + " features in "
        + Fore.CYAN + str(elapsed_time)
        + Style.RESET_ALL + " seconds."
    )

    if args.tgfile is not None:
        t = time.process_time()
        rows, cols = load_talkgroups(args.data_dir, args.tgfile)
        elapsed_time = time.process_time() - t
        print(
            Style.RESET_ALL + "Loaded "
            + Fore.GREEN + str(rows)
            + Style.RESET_ALL + " talkgroups / "
            + Fore.GREEN + str(rows*cols)
            + Style.RESET_ALL + " features in "
            + Fore.CYAN + str(elapsed_time)
            + Style.RESET_ALL + " seconds."
        )


def index(args):
    """Add a record to elastic search"""
    c = Call(args.wavfile, baseurl=args.baseurl, datadir=args.data_dir)
    e = Elasticsearch([args.elasticsearch])
    e.put(c)

    if args.transcribe:
        c.transcribe()

        p = Parser(args.data_dir)
        locs = p.locations(c['transcript'])

        try:
            c['detected_address'] = locs[0].value
            c['location'] = '{}, {}'.format(
                locs[0].point.y,
                locs[0].point.x
            )
        except IndexError:
            pass
        e.put(c)

    return c


def main(parser, args=[]):
    args = parser.parse_args(args)
    try:
        if args.command == "load":
            load(args)
        elif args.command == "index":
            index(args)
        else:
            raise RuntimeError("Unknown command: {}".format(args.command))
    except (ValueError, OSError) as exc:
        parser.error("Unable to {}: {}".format(args.command, exc))
    except (RuntimeError) as exc:
        parser.error(exc)


def entrypoint():
    init()
    parser = make_parser()
    main(parser, sys.argv[1:])


if __name__ == "__main__":
    entrypoint()
