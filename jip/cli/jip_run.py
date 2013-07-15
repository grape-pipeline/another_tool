#!/usr/bin/env python
"""
The JIP job runner command can be used to execute jobs on the local host

Usage:
   jip-run [--cpus <cpus>] [--name <name>] [--output <output>]
           [--input <input>] [--tool <tool> [<args>...]]
   jip-run [--help]

Options:
    -c, --cpus <cpus>              The number of cpus assigned to a job
                                   default: 1
    -n,--name <name>               The job name
    -o,--output <output>           The jobs output file. This will be used as stdout
                                   for the job. In addition, this will automatically
                                   set a jobs 'output' configuraiton property if it
                                   exists
    -i, --input <input>            The jobs stdin is set to this file in case
                                   the tool defines an input names 'input' that
                                   accepts a stream as input. In case the property
                                   is defined but takes only files, this is
                                   configured as property value automatically
    -T, --tool <tool> [<args>...]  The tool to be executed and its arguments

Other Options:
    -h --help             Show this help message
"""
from jip.docopt import docopt


def main():
    import sys
    print sys.argv
    args = docopt(__doc__, options_first=True)
    print args

if __name__ == "__main__":
    main()
