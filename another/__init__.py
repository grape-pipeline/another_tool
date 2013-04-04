#!/usr/bin/env python
"""
Another tool library
====================

The library consists of classes and methods to define computational tools
that can be executed remotely and can be chained to pipelines of tools.
"""
from tempfile import NamedTemporaryFile
import subprocess

from mako.template import Template

class ToolException(Exception):
    """Exception thrown by Tool implementations"""
    pass


class Tool(object):
    """The main tool class that should be extended to implement
    custom python based tools.
    
    In order to create a valid tool, you have to specify a couple of class
    variables that are checked at construction time. The default constructor
    throws a ToolException if mandatory variables are not specified.

    The mandatory class variable you always have to specify are:

    name -- the name of your tool

    Optionally, you can specify any of the following paramters

    short_description -- a short description of the tool
    long_description  -- a long description of the tool
    version           -- a version string 

    In addition to the class variables, the tool implementation must provide
    a call() method implementation that executes the tool. The call method can
    take arbitrary args and kwargs.

    """
    version = None
    long_description = None
    short_description = None

    def __init__(self):
        """Default Tool constructor that checks for the existence of
        all mandatory class variables.
        """
        # check that the name attribute is set
        try:
            getattr(self.__class__, "name")
        except AttributeError:
            raise ToolException("No name specified. Ensure that " \
                    "your tool implementation provides a name class variable")
        # check that the call method is implemented
        try:
            getattr(self, "call")
        except AttributeError:
            raise ToolException("No call() method found. Ensure that " \
                    "your tool implementation provides a name call method " \
                    "implementation")

    def run(self, *args, **kwargs):
        """Default run implementation that does fail checks
        and cleanup. Please do not override this method unless
        you know what you are doing. Implement the call() method
        to implement you r functionality.
        """
        try:
            return self.call(*args, **kwargs)
        except Exception, e:
            raise e

    def __call__(self, *args, **kwargs):
        """Delegate to teh call() implementation of the tool"""
        return self.call(*args, **kwargs) 


class InterpretedTool(Tool):
    """Subclass of :class:Tool that supports a command that
    is rendered to a script and executed by the specified interpreter.

    The interpreter and the command template are specified as class 
    variables. Subclasses have to specify at least the interpreter. The 
    command is optional and can be omitted if a custom implementation of
    the get_command() method is given.

    The default get_command() implementation uses a mako template to render 
    the final result.

    The InterpreterTool also has a return value. The commands returns are
    provided by the _returns() method. By default, this returns the values from
    the returns class variable, but when called, all call parameters are passed
    to the method. Custom implementations of _returns() can use the tools run
    configuration to dynamically return values. 

    In addition, the default implementation of _returns() evaluates functions
    set to the class variable returns. This can be used to quickly and easily
    return dynamic content without overriding _returns(). Passed functions are
    called with the tool instance as first argument followed bu *args and
    **kwargs passed to the call method.

    For example, you can pass a lambda function as return like this:
    
    >>>class MyTool(Tool):
    >>>    name = "MyTool"
    >>>    returns = labmda tool: "my return value"

    """

    interpreter = None
    command = None
    returns = None

    def __init__(self):
        """Ensures that the interpreter is specified and throws 
        a ToolException if not.
        """
        Tool.__init__(self)
        if self.__class__.interpreter is None:
            raise ToolException("No interpreter specified. Ensure that " \
                    "your tool implementation provides a interpreter name!")

    def call(self, *args, **kwars):
        """The interpreter tool call writes the rendered command
        template to a temporary file on disk and calls the interpreter
        with its arguments to run the script. All passed arguments
        are passed to the tools get_command() method to render the template.

        """
        # write the template
        script_file = NamedTemporaryFile()
        script_file.write(self.get_command(*args, **kwargs))
        script_file.flush()

        # try to run the script
        try:
            process = subprocess.Popen([self.__class__.interpreter,
                                        script_file.name], shell=True)
            exit_value = process.wait()
            if exit_value != 0:
                raise ToolException("Interpreter execution failed, " \
                                    "process terminated with %d" % (exit_value))
            return self._returns(*args, **kwargs)
        except Exception, e:
            raise ToolException("Interpreter execution failed " \
                                "due to exception: %s" % (str(e)))
        finally:
            script_file.close()

    def get_command(self, *args, **kwargs):
        """Returns the fully rendered command template. Call args and
        kwargs are given
        """
        return Template(self.__class__.command).render(tool=self, args=args, **kwargs)
    
    def _returns(self, *args, **kwargs):
        """Default implementation of returns evaluates the returns class 
        variable value.

        None is immediately returned. Strings are evaluated as mako templates, 
        putting the tool instance, args and kwargs into the context. Functions 
        are called, passing the tool instance and then args and kwargs as
        paramters. If none of the above applies, the value is returned as is.
        """
        r = self.__class__.returns
        if r is None:
            return None
        if isinstance(r, basestring):
            # render template
            return Template(r).render(tool=self, args=args, **kwargs)
        if callable(r):
            # call function
            return r(self, *args, **kwargs)
        return r
