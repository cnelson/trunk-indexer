import argparse
import time

from colorama import init, Fore, Style

from trunkindexer.gis import GIS

from trunkindexer.storage import Elasticsearch, Call


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

    index = subparsers.add_parser(
        'index',
        help="Add call to elasticseatrch"
    )
    index.add_argument(
        'wavfile',
        help="Path to recording"
    )

    address = subparsers.add_parser(
        'address',
        help="Get location from street address"
    )
    address.add_argument(
        'number',
        type=int,
        help="Street Number"
    )
    address.add_argument(
        'name',
        help="Street Name"
    )

    intersection = subparsers.add_parser(
        'intersection',
        help="Get location from street intersection"
    )
    intersection.add_argument(
        'street1',
        help="First Street"
    )
    intersection.add_argument(
        'street2',
        help="Second Street"
    )

    return parser


def main(parser):
    args = parser.parse_args()

    gis = GIS(args.data_dir)

    try:
        if args.command == "load":
            t = time.process_time()
            num_streets, num_features = gis.load(
                args.gisfile,
                args.street_name,
                args.fromr,
                args.tor,
                args.froml,
                args.tol,
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

        elif args.command == "index":
            c = Call(args.wavfile)
            e = Elasticsearch([args.elasticsearch])
            e.put(c)

            # if args.transcribe:
            #     # TODO: pykaldi
            #     c['transcript'] = "3030 ACTON"

            #     for location in Parser(c['transcript']):
            #         print(location)
            #         c['location'] = location

            #     e.put(c)

        elif args.command == "address":
            s = gis.street(args.name)
            if s is None:
                parser.error("{} is not a valid street.".format(
                    args.name
                ))

            p = s.number_to_location(args.number)
            if p is None:
                parser.error("{} {} is not a valid address.".format(
                    args.number, args.name
                ))

            print(
                Style.RESET_ALL + "{} {}: ".format(args.number, args.name)
                + Fore.CYAN + str(p.y)
                + Style.RESET_ALL + Style.DIM + ", "
                + Style.RESET_ALL + Fore.CYAN + str(p.x)
                + Style.RESET_ALL
            )
        elif args.command == "intersection":
            s1 = gis.street(args.street1)
            s2 = gis.street(args.street2)

            if s1 is None:
                parser.error("{} is not a valid street.".format(
                    args.street1
                ))

            if s2 is None:
                parser.error("{} is not a valid street.".format(
                    args.street2
                ))

            p = s1.intersection(s2)

            if p is None:
                parser.error("{} and {} do not intersect.".format(
                    args.street1, args.street2
                ))

            print(
                Style.RESET_ALL + "{} and {}: ".format(
                    args.street1, args.street2
                )
                + Fore.CYAN + str(p.y)
                + Style.RESET_ALL + Style.DIM + ", "
                + Style.RESET_ALL + Fore.CYAN + str(p.x)
                + Style.RESET_ALL
            )
        else:
            raise RuntimeError("Unknown command: {}".format(args.command))
    except (ValueError, OSError) as exc:
        parser.error("Unable to {}: {}".format(args.command, exc))
    except (RuntimeError) as exc:
        parser.error(exc)

    exit(0)


if __name__ == "__main__":
    init()
    parser = make_parser()
    main(parser)
