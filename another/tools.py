#!/usr/bin/env python
"""
The tools module covers the basic tools and tool definitions. Tools
wrap around executable units and provide the basic functionality to
be integrated into pipeline. The latter covers a listener infrastructure
the allows for callback on certain evens, and validation and completion
methods that can tell the pipeline if a given tool instance is ready
to be executed and valid in terms of its configuration, or, if it
the tool does not need to be executed as its computation is already done and
the results are available.
"""
import logging
from tempfile import NamedTemporaryFile
import subprocess
import signal
import textwrap
import os

from mako.template import Template


class ToolException(Exception):
    """Exception thrown by Tool implementations"""
    def __init__(self, *args):
        Exception.__init__(self, *args)
        self.validation_errors = {}


class Job(object):
    """A Job defines how a tool is executed on a :class:Cluster
    instance. The JobTemplate provides an easy way to create
    a runtime cofiguration and sumbit a job to a cluster using that
    configuration.

    For example:

    >>>tmpl = JobTemplate(threads=8, queue="highload")
    >>>tmpl.submit(cluter, mytool, ...)

    Creates a job template where each job submitted will be executed with
    eight threads and passed to the 'highload' queue. All paramters are
    passed to the cluster and you can achieve the same result by submitting
    the job through the cluster directly, but the template might come
    in handy if you have multiple jobs to submit or you have an environment
    where you have just a few rather fixed templates.
    """

    def __init__(self, template=None, name=None, max_time=0,
                 max_mem=0, threads=1, queue=None,  priority=None,
                 tasks=1, dependencies=None, working_dir=None, extra=None,
                 header=None):
        """Create a new JobTemplate

        Paramter
        --------
        template -- the template that is used to render the job script
        name -- the name of the job
        max_time -- maximum time of the job
        max_mem -- maximum memory of the job
        threads -- number of threads for the job
        queue -- the queue of the job
        priority -- the priority of the job
        tasks -- number of job tasks
        dependencies -- ids of jobs this job depends on
        working_dir -- the jobs worrking directory
        extra -- addition paramters passed to the job
        header -- additional string that is readnered as a haader into the
                  default template
        """
        self.template = template
        self.name = name
        self.max_time = max_time
        self.max_mem = max_mem
        self.threads = threads
        self.queue = queue
        self.priority = priority
        self.tasks = tasks
        self.dependencies = dependencies
        self.working_dir = working_dir
        self.extra = extra
        self.header = header


class Tool(object):
    """The main tool class that should be extended to implement
    custom python based tools.

    In order to create a valid tool, you have to override the `call()`
    method. This will be checked at construction time. The call method
    takes a single dictionary as its first argument. This dictionary contains
    the tool configuration and is also used by other methods, i.e. for
    validation.

    Optionally, you can specify any of the following class variables
    that are used to descripbe the tool:

    name -- the name of your tool
    short_description -- a short description of the tool
    long_description  -- a long description of the tool
    version           -- a version string

    In addition, you can configure the tools paramter by adding class
    variables for `inputs`, `outputs`, and `options`. All of these take
    a dictionary. The keys are used as parameter names and the values
    represent the default settings.

    The tool implementation also consist of a set of listeners:

        * on_start
        * on_success
        * on_fail
        * on_finish

    The listeners are initializesd from the corresponding class variables, but
    you can also add listern functions on the instance.
    A listener is a function implementation that takes the tool instance and
    the configuration as as arguments. The on_start and on_finish
    listeners are always called. on_success and on_fail are called if the tool
    finished successfully or failed.
    All listeners attributes are implemented as lists and you should append
    new listeners to the lists. For example

    >>> def my_start_listener(tool, args):
    >>> ... print "Tool %s started" % (tool)
    >>> mytool.on_start.append(my_start_listener)

    This adds a new on_start method, which will be called just before the tools
    call() method is executed.

    By default, the tools class starts listening to SIGINT and SIGTERM singnals
    and call the cleanup() and on_fail listeners. A signals of that sort is
    treated as a execution failure. You can disable the signal handeling on
    a class or instance level by setting the handle_signals class or instance
    attribute to False.

    """
    version = None
    long_description = None
    short_description = None
    on_start = []
    on_finish = []
    on_success = []
    on_fail = []
    handle_signals = True
    inputs = {}
    outputs = {}
    options = {}

    def __init__(self, name=None):
        """Default Tool constructor creates a new instance and optionally
        assignes a name to the tool instance. If the name is not specified,
        the class is checked for a `name` class variable. If that is not found,
        the class name is used.

        Paramter
        --------
        name -- optional name for this tool instance
        """
        # check name and call method
        self.__check_name(name)
        self.__check_call_method()

        self.job = Job()

        # initialize listener list
        self.on_start = []
        self.on_start.extend(self.__class__.on_start)
        self.on_finish = []
        self.on_finish.extend(self.__class__.on_finish)
        self.on_fail = []
        self.on_fail.extend(self.__class__.on_fail)
        self.on_success = []
        self.on_success .extend(self.__class__.on_success)

        # copy signals attribute
        self.handle_signals = self.__class__.handle_signals

        # copy options, inputs and outputs
        self.outputs = {}
        if self.__class__.outputs is not None:
            self.outputs.update(self.__class__.outputs)
        self.inputs = {}
        if self.__class__.inputs is not None:
            self.inputs.update(self.__class__.inputs)
        self.options = {}
        if self.__class__.options is not None:
            self.options.update(self.__class__.options)

        self.log = logging.getLogger(self.name)

    def __check_call_method(self):
        """Check the tool for an existing call method implementation"""
        try:
            getattr(self, "call")
        except AttributeError:
            raise ToolException(
                "No call() method found. Ensure that "
                "your tool implementation provides a name call method "
                "implementation"
            )

    def __check_name(self, name):
        """Check the tools name and assigne a default one."""
        if name is not None:
            self.name = name
            return

        try:
            clname = getattr(self.__class__, "name")
            self.name = clname
        except AttributeError:
            self.name = self.__class__.__name__

    def run(self, args):
        """Default run implementation that does fail checks
        and cleanup. It takes the tools configuration as argument
        and returns the result of the tools `call()` method implementation.

        Please do not override this method unless
        you know what you are doing. Implement the call() method
        to implement your functionality.

        Paramter
        --------
        args - the tools configuration dictionary
        """
        # maintain a state array where we can put
        # states to avoid conflicts between
        # signal handler and normal listener calls
        state = []
        if self.handle_signals:
            # add singnal handler
            #
            # to be able to pass the args and kwargs to
            # the listeners that are called by the handler,
            # we need them in an array
            handler_data = [self, args]

            def handler(signum, frame):
                # on signal, append true to the handler data
                # to indicate that the handler managed
                # the cleanup and lister calls
                if "cleanup" not in state:
                    state.append("cleanup")
                    handler_data[0].cleanup(handler_data[1], failed=True)
                if "on_fail" not in state:
                    state.append("on_fail")
                    handler_data[0]._on_fail(handler_data[1])
                if "on_finish" not in state:
                    state.append("on_finish")
                    handler_data[0]._on_finish(handler_data[1])

            signal.signal(signal.SIGHUP, handler)
            signal.signal(signal.SIGTERM, handler)
        return self.__execute(state, args)

    def validate(self, args, incoming=None):
        """Validate the input paramters and throw an ToolException
        in case of any errors.
        The exception message should carry details about the configuration
        issues encounterd.

        Note that the default implementation always return True and
        no validation happens. Override this if you want to implement
        custom validation.

        If the `incoming` paramter is set, it contains the configuraiton
        dictionary that contains fields that are set by incoming
        dependencies if this tool is part of a pipeline. The validation
        implementation should check for this to, for example, not fail
        if files do not exists that come from an incoming dependencies. In
        these cases, the file should get created by the dependencies before
        this tool is executed, but we can not validate that before the
        actual execution.

        Paramter
        --------
        args - the tools configuration dictionary
        """
        return True

    def is_done(self, args):
        """Return True if no execution is needed for this tool.
        The default implementations does not perform any checks and
        always returnsm False
        """
        return False

    def __execute(self, state, args):
        """Internal method that does the actual execution of the
        call method after singlan handler are set up.
        This method is responsible for calling the listeners
        and executing call. It returns the call return value.

        Parameter
        ---------
        state - the state list that is used to synchronize listner calls
                between this method and any signal handlers
        args  - the tools configuration dictionary
        """
        # call the start up listeners
        self._on_start(args)
        # class the call method
        try:
            result = self.call(args)
            self._on_success(args)
            # successful call, do cleanup
            if "cleanup" not in state:
                state.append("cleanup")
                self.cleanup(args, failed=False)
            return result
        except Exception, e:
            if "on_fail" not in state:
                state.append("on_fail")
                self._on_fail(args)
            # do cleanup on failed call
            if "cleanup" not in state:
                state.append("cleanup")
                self.cleanup(args, failed=True)
            raise e
        finally:
            if "on_finish" not in state:
                state.append("on_finish")
                self._on_finish(args)

    def _on_start(self, args):
        """Method called just before execution of the call method is executed
        and executes all on_start listener.
        """
        self.__call_listener(self.on_start, args)

    def _on_finish(self, args):
        """Method called after execution of the call method is executed and
        executes all on_finish listeners.
        """
        self.__call_listener(self.on_finish, args)

    def _on_fail(self, args):
        """Method called after execution of the call method is executed but
        only if the call failed and raised an exception. It calls all
        on_fail listeners.
        """
        self.__call_listener(self.on_fail, args)

    def _on_success(self, args):
        """Method called after execution of the call method is executed but
        only if the call finsihed successfully without raising an exception.
        It calls all on_success listeners.
        """
        self.__call_listener(self.on_success, args)

    def __call_listener(self, listener_list, args):
        """Call the listeners in the given listener list. If
        the list is None, nothing is called.
        """
        if listener_list is not None:
            for listener in listener_list:
                try:
                    listener(self, args)
                except Exception, e:
                    self.log.warn("Listener call %s failed with"
                                  " exception: %s", listener, e)

    def cleanup(self, args, failed=False):
        """Cleanup method that is called after a run. The failed paramter
        indicates if the run failed or not. The default implemetation
        does no cleanup operations. Override this method to implement
        custom cleanup for a tool.

        Paramter
        --------
        args   - the tools configuration dictionary
        failed - True if the execution failed
        """
        pass

    def _get_value(self, name):
        """Checks inputs, outputs, and options in that order for the
        named value and raises an AttributeError if the name does not
        exist
        """
        if name in self.inputs:
            return self.inputs[name]
        if name in self.outputs:
            return self.outputs[name]
        if name in self.options:
            return self.options[name]

        raise AttributeError("%r object has no attribute %r" %
                             (type(self).__name__, name))

    def _get_default_configuration(self):
        """Return the configuraiton dict for this tool
        with all values set to default. All values
        are unresolved but the outputs are taken from
        returns()

        """
        config = {}
        config.update(self.inputs)
        config.update(self.outputs)
        config.update(self.options)
        return config


class InterpretedTool(Tool):
    """Subclass of :class:Tool that supports a command that
    is rendered to a script and executed by the specified interpreter.

    The interpreter and the command template are specified as class
    variables. Subclasses have to specify at least the interpreter. The
    command is optional and can be omitted if a custom implementation of
    the get_command() method is given.

    The default get_command() implementation uses a mako template to render
    the final result.

    The InterpreterTool also has a return value. The commands outputs are
    provided by the returns() method. By default, this returns the values from
    the `outputs` class variable, but when called, all call parameters are
    passed to the method. Custom implementations of returns() can use the
    tools run configuration to dynamically return values.

    In addition, the default implementation of returns() evaluates functions
    set to the class variable returns. This can be used to quickly and easily
    return dynamic content without overriding returns(). Passed functions are
    called with the tool instance as first argument followed bu *args and
    **kwargs passed to the call method.

    For example, you can pass a lambda function as return like this:

    >>>class MyTool(Tool):
    >>>    name = "MyTool"
    >>>    returns = labmda tool: "my return value"

    """

    interpreter = None
    command = None

    def __init__(self, name=None):
        """Ensures that the interpreter is specified and throws
        a ToolException if not.
        """
        Tool.__init__(self, name=name)
        if self.__class__.interpreter is None:
            raise ToolException("No interpreter specified. Ensure that "
                                "your tool implementation provides a "
                                "interpreter name!")

    def call(self, args):
        """The interpreter tool call writes the rendered command
        template to a temporary file on disk and calls the interpreter
        with its arguments to run the script. All passed arguments
        are passed to the tools get_command() method to render the template.

        """
        # write the template
        script_file = NamedTemporaryFile()
        script_file.write(self.get_command(args))
        script_file.flush()

        # try to run the script
        try:
            process = subprocess.Popen([self.__class__.interpreter,
                                        script_file.name], shell=False)
            exit_value = process.wait()
            if exit_value != 0:
                raise ToolException("Interpreter execution failed, process"
                                    " terminated with %d" % (exit_value))
            return self.returns(args)
        except Exception, e:
            raise ToolException("Interpreter execution failed "
                                "due to exception: %s" % (str(e)))
        finally:
            script_file.close()

    def get_command(self, args):
        """Take the tools configuration dictionary and returns the string
        representation of the command script.
        """
        if args is None:
            args = {}
        args["job"] = self.job
        rendered = Template(self.__class__.command).render(tool=self, **args)
        return textwrap.dedent(rendered)

    def is_done(self, args):
        """Returns true if the tools has outputs defined and
        all outputs exist
        """
        outs = self.returns(args)
        if outs is None:
            return False
        for output in outs:
            if output is not None and len(output) > 0:
                if not os.path.exists(output):
                    return False
        return True

    def validate(self, args, incoming=None):
        """Validate the interpreted tool options based on the `inputs`.
        If `inputs` is not defined, this always returns True, otherwise
        False is returned if one if the kyes in `inputs` is not contained
        in the kwargs"""
        if self.__class__.inputs is None:
            return True

        for k, v in self.__class__.inputs.items():
            if not k in args:
                return False
        return True

    def returns(self, args):
        """Default implementation of returns evaluates the returns class
        variable value.

        If the default output is None, the method immediately returneds.
        Strings are evaluated as mako templates, putting the tool instance
        and the tools configuration dictionary into the context.
        Functions are called, passing the tool instance and the tool
        configuraiton as arguments. If none of the above applies, the value
        is returned as is.
        """
        rets = []
        r = self.outputs
        if r is None:
            return None

        if isinstance(r, dict):
            # interprete it as a map and evaluate the values
            for k, v in r.items():
                ev = self.__evaluate_returns_value(v, args)
                if ev is not None:
                    if isinstance(ev, (list, tuple,)):
                        rets.extend(ev)
                    else:
                        rets.append(ev)
        else:
            ev = self.__evaluate_returns_value(r, args)
            if ev is not None:
                if isinstance(ev, (list, tuple,)):
                    rets.extend(ev)
                else:
                    rets.append(ev)
        if len(rets) == 0:
            return None
        return rets

    def _resolve(self, args):
        rets = {}
        for k, v in args.items():
            ev = self.__evaluate_returns_value(v, args)
            rets[k] = ev
        return rets

    def __evaluate_returns_value(self, r, args):
        """Evaluate a single return value that is set in the `outputs`
        class variable. Strings are treated as templates with access to the
        current context, functions are executed with the current context
        """
        if r is None:
            return None
        if args is None:
            args = {}
        if isinstance(r, basestring):
            # render template
            return Template(r).render(tool=self, **args)
        if callable(r):
            # call function
            return r(args)
        return r

    def cleanup(self, args, failed=False):
        """The default cleanup method of an interpreted tool checks the tools
        returns() and removes any files it finds
        """
        if failed:
            rets = self.returns(args)
            files = []
            if isinstance(rets, basestring):
                files.append(rets)
            elif isinstance(rets, (list, tuple,)):
                for r in rets:
                    if isinstance(r, basestring):
                        files.append(r)

            for f in files:
                if os.path.exists(f):
                    os.remove(f)


class BashTool(InterpretedTool):
    """Interpreter tool that uses bash for execution"""
    interpreter = "bash"