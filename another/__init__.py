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

    The tool implementation also consist of a set of listeners:

        * on_start
        * on_success
        * on_fail
        * on_finish

    The listeners are initializesd from the corresponding class variables, but
    you can also add listern functions on the instance.
    A listener is a function implementation that takes the tool instance and
    all the call *args and ** kwargs as arguments. The on_start and on_finish
    listeners are always called. on_success and on_fail are called if the tool
    finished successfully or failed.
    All listeners attributes are implemented as liste and you should append
    new listeners to the lists. For example

    >>> def my_start_listener(tool, *args, **kwargs):
    >>> ... print "Tool %s started" % (tool)
    >>> mytool.on_start.append(my_start_listener)

    This adds a new on_start method, which will be called just before the tools
    call() method is executed.

    """
    version = None
    long_description = None
    short_description = None
    on_start = []
    on_finish = []
    on_success = []
    on_fail = []

    def __init__(self):
        """Default Tool constructor that checks for the existence of
        all mandatory class variables.
        """
        # check that the name attribute is set
        try:
            getattr(self.__class__, "name")
        except AttributeError:
            raise ToolException("No name specified. Ensure that "
                                "your tool implementation provides "
                                "a name class variable")
        # check that the call method is implemented
        try:
            getattr(self, "call")
        except AttributeError:
            raise ToolException(
                "No call() method found. Ensure that "
                "your tool implementation provides a name call method "
                "implementation"
            )
        # initialize listener list
        self.on_start = []
        self.on_start.extend(self.__class__.on_start)
        self.on_finish = []
        self.on_finish.extend(self.__class__.on_finish)
        self.on_fail = []
        self.on_fail.extend(self.__class__.on_fail)
        self.on_success = []
        self.on_success .extend(self.__class__.on_success)

    def run(self, *args, **kwargs):
        """Default run implementation that does fail checks
        and cleanup. Please do not override this method unless
        you know what you are doing. Implement the call() method
        to implement you r functionality.
        """
        # call the start up listeners
        self._on_start(*args, **kwargs)
        # class the call method
        try:
            result = self.call(*args, **kwargs)
            self._on_success(*args, **kwargs)
            # successful call, do cleanup
            self.cleanup(failed=False)
            return result
        except Exception, e:
            self._on_fail(*args, **kwargs)
            # do cleanup on failed call
            self.cleanup(failed=True)
            raise e
        finally:
            self._on_finish(*args, **kwargs)

    def _on_start(self, *args, **kwargs):
        """Method called just before execution of the call method is executed.
        The method checks the list of on_start functions associated with the
        tool class and passes on the tool instance and all args and kwargs
        to all function in the listener list.
        """
        self._call_listener(self.on_start, *args, **kwargs)

    def _on_finish(self, *args, **kwargs):
        """Method called after execution of the call method is executed.
        The method checks the list of on_finish functions associated with the
        tool class and passes on the tool instance and all args and kwargs
        to all function in the listener list.
        """
        self._call_listener(self.on_finish, *args, **kwargs)

    def _on_fail(self, *args, **kwargs):
        """Method called after execution of the call method is executed but
        only if the call failed and raised an exception.
        the method checks the list of on_fail functions associated with the
        tool class and passes on the tool instance and all args and kwargs
        to all function in the listener list.
        """
        self._call_listener(self.on_fail, *args, **kwargs)

    def _on_success(self, *args, **kwargs):
        """Method called after execution of the call method is executed but
        only if the call finsihed successfully without raising an exception.
        the method checks the list of on_success functions associated with the
        tool class and passes on the tool instance and all args and kwargs
        to all function in the listener list.
        """
        self._call_listener(self.on_success, *args, **kwargs)

    def _call_listener(self, listener_list, *args, **kwargs):
        """Call the listeners in the given listener list. If
        the list is None, nothing is called. The args and kwargs
        are passed to the listener.
        """
        if listener_list is not None:
            for listener in listener_list:
                try:
                    listener(self, *args, **kwargs)
                except:
                    # todo: add logging to report failing listener
                    pass

    def cleanup(self, failed=False):
        """Cleanup method that is called after a run. The failed paramter
        indicates if the run failed or not.
        """
        pass

    def __call__(self, *args, **kwargs):
        """Delegate to teh call() implementation of the tool"""
        return self.run(*args, **kwargs)


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
            raise ToolException("No interpreter specified. Ensure that "
                                "your tool implementation provides a "
                                "interpreter name!")

    def call(self, *args, **kwargs):
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
                raise ToolException("Interpreter execution failed, process"
                                    " terminated with %d" % (exit_value))
            return self._returns(*args, **kwargs)
        except Exception, e:
            raise ToolException("Interpreter execution failed "
                                "due to exception: %s" % (str(e)))
        finally:
            script_file.close()

    def get_command(self, *args, **kwargs):
        """Returns the fully rendered command template. Call args and
        kwargs are given
        """
        return Template(self.__class__.command).render(tool=self,
                                                       args=args, **kwargs)

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
