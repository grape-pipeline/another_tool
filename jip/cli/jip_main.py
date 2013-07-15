#!/usr/bin/env python
"""
The JIP comand line utility

Usage:
    jip [--verbose] [--version] [--help] <command> [<args>...]

Options:
    -h --help     Show this help message
    -v --verbose  Be verbose when taking action
    --version     Show the slurm version information

The most commonly used slurm commands:

    run  run a jip job
"""
import sys
import jip
from jip.docopt import docopt


def main():
    args = docopt(__doc__, version=str(jip.__version__),
                  options_first=True, help=True)

    #todo: handle --verbose
    cmd = args['<command>']
    original_args = sys.argv
    try:
        import runpy
        argv = ["jip-" + cmd] + args['<args>']
        sys.argv = argv  # reset options
        runpy.run_module("jip.cli.jip_%s" % cmd, run_name="__main__")
    except Exception:
        # check interpreter mode
        import os
        if os.path.exists(cmd):
            import runpy
            argv = ["jip-interpreter"] + [cmd] + args['<args>']
            sys.argv = argv  # reset options
            runpy.run_module("jip.cli.jip_interpreter", run_name="__main__")
        else:
            sys.stderr.write("\nCommand %s not found\n\n" % (cmd))
            docopt(__doc__, version='1.0', options_first=True, argv=['--help'],
                   help=True)
            sys.exit(0)
