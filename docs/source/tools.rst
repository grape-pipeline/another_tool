Tools and tool implementations
==============================
Tools are the smallest executable units you can implement in a pipeline
setup. A *full* tool implementation carries the tools execution logic as well
as information about the tools configuration and some custom validation logic
that ensures that the tools configuration is valid and the tool ca be
executed.

The main tool class that should be extended to implement
custom tools that execute logic is :class:`another.tools.Tool`. The tools
logic must be implemented in the :func:`call` method.
The constructor checks for an implementation of the :func:`call` method and a
:class:`ToolException` is raised if no implementation is found.

All tools are configured with a single dictionary, the tool *configuration*.
This configuration is made available to all methods that execute or validate
the tool or that are involved in the tools execution flow.

The simplest way to create custom tool implementation that is based on
python code just implements the :func:`call` method. For example:

    >>> from another.tools import Tool
    >>> class MyTool(Tool):
    ...     def call(self, args):
    ...         print "MyTool is configured with", args

The common execution flow of a tool consists of a validation step and an
execution step. This already works with the example above:

    >>> tool = MyTool()
    >>> cfg = {"a": 1, "b": 2}
    >>> tool.validate(cfg)
    True
    >>> tool.run(cfg)
    MyTool is configured with {'a': 1, 'b': 2}

Note that the default implementation of :func:`another.tool.Tool.validate`
does not perform any checks. Note also that we call the tools :func:`run`
method to execute the tool rather then the :func:`another.tool.Tool.call` method
that we just implemented. This is due to the fact that the tool supports a
listener chain where we can add various methods that are executed while the
tool goes through its execution life cycle.

Tool validation
---------------
A tool implementation can provide a custom validation function that can
check the tools configuration and raise a :class:`ValidationException` in
case any errors are encountered. For example, we can ensure that our tool
configuration contains an ``input_file`` parameter:

    >>> from another.tools import Tool, ValidationException
    >>> class MyTool(Tool):
    ...     def call(self, args):
    ...         pass
    ...     def validate(self, args, incoming=None):
    ...         if "input_file" not in args:
    ...             raise ValidationException({"input_field": "No input file specified"})
    ...
    >>> mytool = MyTool()
    >>> mytool.validate({})
    Traceback (most recent call last):
      File "<stdin>", line 1, in <module>
      File "<stdin>", line 6, in validate
    another.tools.ValidationException: Validation Error

You may have noticed the *incoming* parameter. This is an additional
parameter that is set if your tool implementation is executed within a
pipeline setup.
Say your tool acts on an ``input_file`` that is passed as a configuration
parameter. For direct execution outside of any pipeline,
you might want to check if the file exists in your validation
function. This is of course good practice for the standalone tool
implementation but will brake in cases where the tool is executed
withing a pipeline of other tools and the ``input_file`` is generated in
a previous step of the pipeline. In this case, while the pipeline is
validated, the file can not exists yet. These *incoming* dependency are
passed into the validation so you can check if a certain parameter is
passed in via dependency and then modify your validation code. For example:

    >>> class MyTool(Tool):
    ...     def call(self, args):
    ...         pass
    ...     def validate(self, args, incoming=None):
    ...         if incoming is None:
    ...             incoming = {}
    ...         if "input_file" not in args:
    ...                 raise ValidationException({"input_field": "No input file specified"})
    ...         if "input_file" not in incoming and
    ...            not os.path.exists(args["input_file"]):
    ...                raise ValidationException({"input_field": "Input file not found"})

Here, the ``input_file`` existence is only checked if it is not contained
in *incoming* and therefore it is an explicit dependency rather than an
implicit one that is resolved by another pipeline step.

Flow of execution
-----------------
The reason we called the :func:`run` method to execute a tool rather then the
:func:`call` implementation directly is two fold. First,
``run()`` takes care of error handling, second, a chain of (optional)
listener functions is called. The basic execution flow looks like this:

1. Set up signal handling to properly cancel the tool execution
2. Call the ``on_start`` listeners
3. Execute the tools :func:`call` method
4. If an exception is raised or a signal is received:
     - call the ``on_fail`` listeners
     - call :func:`cleanup` with ``failed=True``
   Else
     - call the ``on_success`` listeners
     - call :func:`cleanup` with ``failed=False``
5. Call the `on_finish` listeners

This implies that you can associate the following listeners with your tools:

 * on_start
 * on_success
 * on_fail
 * on_finish

The listeners are initialised from the corresponding class variables, but
you can also add listener functions on the instance.
A listener is a function implementation that takes the tool instance and
the configuration as as arguments. The ``on_start`` and ``on_finish``
listeners are always called. ``on_success`` and ``on_fail`` are called if the
tool finished successfully or failed.
All listeners attributes are implemented as lists and you should append
new listeners to the lists to add them. For example:

    >>> def my_start_listener(tool, args):
    >>> ... print "Tool %s started" % (tool)
    >>> mytool.on_start.append(my_start_listener)

This adds a new on_start method, which will be called just before the tools
call() method is executed.

By default, the tools class starts listening to SIGINT and SIGTERM signals
and call the cleanup() and on_fail listeners. A signals of that sort is
treated as a execution failure. You can disable the signal handling on
a class or instance level by setting the :attr:`handle_signals` class or
instance attribute to ``False``.


Classes
-------

.. autoclass:: another.tools.Tool
    :members:

.. autoclass:: another.tools.Job
    :members:

Exceptions and Errors
---------------------

.. autoclass:: another.tools.ToolException
    :members:

.. autoclass:: another.tools.ValidationException
    :members:
