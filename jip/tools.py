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

How a tool is executed locally or on a remote cluster can be further
specified using the tools `job` attribute. An instance of
:class:`jip.tools.Job` is associated with each tool instance.
"""

import signal
import os


class ToolException(Exception):
    """This is the default exception raised by tool implementations.

    In addition to the error message, the exception might carry additional
    information. The following attributes might be set when the exception is
    raised:

    *termination_signal* -- Defaults to None and set to the signal number in
    case the tool run was terminated by a signal

    *exit_value* -- Defaults to 0 and is set to the exit value of the process
    when the exception is raised during execution
    """
    def __init__(self, *args):
        Exception.__init__(self, *args)
        self.termination_signal = None
        self.exit_value = 0


class ValidationException(Exception):
    """The validation exception is raised by :func:`Tool.validate`
    implementations in case a validation error occurred. The errors
    dictionary contains the invalid field names as keys and a more explicit
    error messages as value.

    Properties:
        errors: dictionary
            The error dict where keys are the invalid field names and values
            are the error messages
    """
    def __init__(self, errors):
        """
        Create a new instance of Validation exception

        :param errors: the errors
        :type errors: dict
        """
        Exception.__init__(self, "Validation Error")
        self.errors = errors

    def __unicode__(self):
        return self.__str__()

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        s = "Pipeline Validation failed!\n"
        if self.errors is None:
            return s
        for field, msg in self.errors.items():
            s += "%s\t: %s\n" % (field, msg)
        return s


class Job(object):
    """An instance of Job defines how a :class:`Tool` is executed on a
    :class:`jip.cluster.Cluster` instance or locally. The Job provides an
    easy way to create a runtime configuration that is then evaluated during
    job submission or execution.

    In addition to default parameters such as the number of threads or the
    working directory, also general submission parameters can be set. This
    covers the basic template that is used to run a job in the cluster,
    and the *header* extension of the default template. For more details on
    the template and the header extension, see :class:`jip.cluster.Cluster`.

    Properties:
        template: string
            The template that is used to render the job script
        name: string
            The name of the job
        max_time: integer
            The maximum wall clock time in minutes allowed for the job
        max_mem: integer
            The maximum memory in MB allowed for the job
        threads: integer (default 1)
            Number of threads/cpus assigned to the job
        tasks: integer (default 1)
            Number of tasks assigned to the job
        queue: string
            The queue that should be used to run the job
        priority: string
            The priority that should be used to run the job
        dependencies: list of ids or list of tools
            Either a list of id's of jobs this job depends on,
            or a list of other tool instances this job depends on.
        working_dir: string
            The jobs working directory
        extra: list of string
            Extra attributes passed to the cluster submission
        header: string
            Additional string that is rendered as is into the jobs
            templates before the job execution commands. This can be use,
            for example, to set up the jobs environment at runtime
        verbose: boolean (default True)
            If set to False, all job output is hidden
        logdir: string
            The base directory where log files should be put
        jobid: string
            The job id. This is set after the job was submitted to a remote
            cluster
    """

    def __init__(self):
        self.template = None
        self.name = None
        self.max_time = None
        self.max_mem = None
        self.threads = 1
        self.queue = None
        self.priority = None
        self.tasks = 1
        self.dependencies = []
        self.working_dir = None
        self.extra = []
        self.header = None
        self.verbose = True
        self.logdir = None
        self.jobid = None


class ToolMetaClass(type):
    """Tool meta class to be able to
    set class level properties that have mutable lists or dictionaries
    as default values
    """
    __list_values = ["on_start", "on_success", "on_fail", "on_finish"]
    __dict_values = ["inputs", "outputs", "options"]

    def __getattr__(cls, name):
        value = None
        found = False
        if name in ToolMetaClass.__list_values:
            value = []
            found = True
        if name in ToolMetaClass.__dict_values:
            value = {}
            found = True
        if found:
            setattr(cls, name, value)
            return value
        raise AttributeError("Attribute %s not found" % name)


class Tool(object):
    __metaclass__ = ToolMetaClass
    name = None
    version = None
    long_description = None
    short_description = None
    handle_signals = True
    interpreter = "bash"
    command = None

    def __init__(self, name=None):
        """Default Tool constructor creates a new instance and optionally
        assigns a name to the tool instance. If the name is not specified,
        the class is checked for a `name` class variable. If that is not found,
        the class name is used.

        Parameter:
            name: string (optional)
                The name of the tool instance. Defaults to the tool name
                class attribute
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

        # save signals
        self._received_signal = None

        # setup the process and check for the interpreter
        self.__process = None
        if self.__class__.interpreter is None:
            raise ToolException("No interpreter specified. Ensure that "
                                "your tool implementation provides a "
                                "interpreter name!")


    @property
    def log(self):
        import logging
        return logging.getLogger(self.name)

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
            if clname is not None:
                self.name = clname
            else:
                self.name = self.__class__.__name__
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
                #save signla
                handler_data[0]._received_signal = signum
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
            signal.signal(signal.SIGINT, handler)
        return self.__execute(state, args)

    def __execute(self, state, args):
        """Internal method that does the actual execution of the
        call method after signal handler are set up.
        This method is responsible for calling the listeners
        and executing call. It returns the call return value.

        Parameter
        ---------
        state - the state list that is used to synchronize listener calls
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
            raise ToolException("Tool execution of %s failed : %s" % (self.name, str(e)), e)
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
            import inspect
            for listener in listener_list:
                try:
                    # make sure listener is a function or a callable
                    inspect_target = listener
                    if not inspect.isfunction(listener):
                        inspect_target = listener.__call__

                    # gues the parameter arguments
                    argspec = inspect.getargspec(inspect_target)
                    if len(argspec.args) == 0:
                        listener()
                    elif len(argspec.args) == 1:
                        listener(self)
                    else:
                        if argspec.keywords is not None:
                            listener(self, **args)
                        else:
                            listener(self, args)
                except Exception, e:
                    self.log.warn("Listener call %s failed with"
                                  " exception: %s", listener, e)

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
        False is returned if one if the keys in `inputs` is not contained
        in the kwargs"""
        if self.inputs is None:
            return True

        errors = {}
        for k, v in self.inputs.items():
            if not k in args:
                errors[k] = "Configuration value not specified"
            else:
                v = args[k]
                if v is None:
                    errors[k] = "No value specified for %s" % (k)
        if len(errors) > 0:
            raise ValidationException(errors)

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
        local_outputs = self.outputs
        if local_outputs is None:
            return None

        if isinstance(local_outputs, dict):
            # interprete it as a map and evaluate the values
            for k, v in local_outputs.items():
                # override the value from args if its there
                if args is not None and k in args and args[k] is not None:
                    v = args[k]
                ev = self.__evaluate_returns_value(v, args)
                if ev is not None:
                    if isinstance(ev, (list, tuple,)):
                        rets.extend(ev)
                    else:
                        rets.append(ev)

        else:
            ev = self.__evaluate_returns_value(local_outputs, args)
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
            from mako.template import Template
            t = Template(r).render(tool=self, **args)
            return t
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

    def call(self, args):
        """The interpreter tool call writes the rendered command
        template to a temporary file on disk and calls the interpreter
        with its arguments to run the script. All passed arguments
        are passed to the tools get_command() method to render the template.

        """
        # write the template
        from tempfile import NamedTemporaryFile
        import subprocess

        script_file = NamedTemporaryFile()
        script_file.write(self.get_command(args))
        script_file.flush()

        # try to run the script
        try:
            stdout = None
            stderr = None
            if not self.job.verbose:
                # pipe to /dev/null
                stdout = open("/dev/null", "w")
                stderr = open("/dev/null", "w")

            self.__process = subprocess.Popen([self.__class__.interpreter,
                                               script_file.name], shell=False,
                                              stdout=stdout,
                                              stderr=stderr)
            exit_value = self.__process.wait()
        except Exception, e:
            # kill the process
            if self.__process is not None:
                try:
                    self.__process.kill()
                except OSError:
                    pass  # silent try to kill
            raise ToolException("Interpreter execution failed "
                                "due to exception: %s" % (str(e)))
        else:
            if exit_value != 0 or self._received_signal is not None:
                if self._received_signal is not None:
                    exp = ToolException("Interpreter execution failed, process"
                                        " terminated with signal %d" %
                                        (self._received_signal))
                else:
                    exp = ToolException("Interpreter execution failed, process"
                                        " terminated with %d" % (exit_value))
                exp.exit_value = exit_value
                exp.termination_signal = self._received_signal
                raise exp
            return self.returns(args)
        finally:
            script_file.close()

    def get_command(self, args):
        """Take the tools configuration dictionary and returns the string
        representation of the command script.
        """
        import textwrap
        from mako.template import Template

        if args is None:
            args = {}
        args["job"] = self.job
        rendered = Template(self.__class__.command).render(tool=self, **args)
        return textwrap.dedent(rendered)
