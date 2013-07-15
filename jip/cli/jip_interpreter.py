#!/usr/bin/env python
"""
The JIP job interpreter command can be used to execute jobs on the local host

Usage:
   jip-interpreter <file> [<args>...] [-- [<jip_args>...]]
   jip-interpreter [--help|-h]

Options:
    <file>  the jip script

Other Options:
    -h --help             Show this help message
"""
from jip.docopt import docopt
from jip.scripts import scripts


def split_to_jip_args(args):
    """Check the <args> and search for '--'. If found,
    everything after '--' is put into 'Jip_args'"""
    if args and "<args>" in args:
        try:
            i = args["<args>"].index("--")
            args["<args>"] = args["<args>"][:i]
            args["<jip_args>"] = args["<args"][i + 1:]
        except ValueError:
            pass


def main():
    args = docopt(__doc__, options_first=True)
    # split args and jip_args
    split_to_jip_args(args)
    script_file = args["<file>"]
    script = scripts.parse_script(script_file)


if __name__ == "__main__":
    main()
