#!/usr/bin/env python
"""The executor tool is responsible for moving tools for execution to
and from different compute environment.

This module allows to serialize a tool and create a portable bash script
that can be called independently in other environments. The script will call
the runner module back, where the pickeled module is loaded, initialized and
executed.
"""
import cPickle


class ToolWrapper(object):
    """Tool wrapper class that holds the tool and
    the args and kwargs of it. The class provides
    a single run() method that will execute the tool
    """

    def __init__(self, tool, args, kwargs):
        """Initialize the wrapper with the tool and its arguments"""
        self.tool = tool
        self.args = args
        self.kwargs = kwargs

    def run(self):
        """Run the tool"""
        self.tool.run(*(self.args), **(self.kwargs))


def dump(tool, *args, **kwargs):
    """Save the given tool instance and the arguments and returns a string
    that is a valid bash script that will load and execute the tool.

    Note that the script does not set up the python environment or
    the paths. This has to be done by the script caller!

    Parameter
    ---------

    tool -- the tool that will be prepared for execution
    *args -- tool arguments
    **kwargs -- tool keyword arguments
    Returns
    -------

    script -- a string that is a valid bash script and will load and
              run the tool
    """
    template = """
python -c '
import sys;
import cPickle;
source="".join([l for l in sys.stdin]).decode("base64");
result = cPickle.loads(source).run();
# pickel the result and print it to stdout
result_string = cPickle.dumps(result).encode("base64");
print "-------------------RESULT-------------------"
print result_string
'<< __EOF__
%s__EOF__

"""
    wrapper = ToolWrapper(tool, args, kwargs)
    return template % cPickle.dumps(wrapper).encode("base64")
