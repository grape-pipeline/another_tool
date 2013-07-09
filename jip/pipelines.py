#!/usr/bin/env python
"""Another tool pipeline implementation to create pipelines of tools.
"""
from functools import reduce
from jip.tools import ValidationException


class PipelineException(Exception):
    """Common error message raised by pipelines.
    The pipeline exception contains a validation_errors field
    that holds a dictionary to store any pipeline validation errors.
    """
    def __init__(self, *args, **kwargs):
        Exception.__init__(self, *args)
        self.validation_errors = {}
        self.circular_dependencies = {}
        if "circular_dependencies" in kwargs:
            self.circular_dependencies = kwargs["circular_dependencies"]

    def __repr__(self):
        if len(self.validation_errors) > 0:
            s = "Pipeline validation failed for: \n\n"
            for tool, errors in self.validation_errors.items():
                s += "%s:\n" % (tool)
                for field, msg in errors.items():
                    s += "\t%s\t: %s" % (field, msg)
            return s
        return Exception.__repr__(self)

    def __str__(self):
        return self.__repr__()



class CircularDependencyException(Exception):
    """Exception that is raised by the pipeline when a circular
    dependency is detected"""
    def __init__(self, circle):
        self.circle = circle

    def __repr__(self):
        return "Circular dependency: %s" % ("->".join(self.circle))


class Pipeline(object):
    """The pipeline class wrapps around a list of tools
    and manages dependencies and paramters. You can add tools
    to the pipeline using the add() method. This returns a new
    :class:PipelineTool instance that can be used to configure the tool
    and create dependencies. For example:

        >>> pipeline = Pipeline()
        >>> a = pipeline.add(MyTool(), "a")
        >>> a.input = "myfile.txt"

    Here we create a new pipeline and add a tool we call "a". We specify
    the name explicitly here, otherwise the tools name is used. Then
    we assign the `input` value for the tool. The name is important here
    as it can be used to lookup tools in the pipeline:

        >>> b = pipeline.get("a")
        >>> assert a == b

    """
    def __init__(self, name="pipeline"):
        self.tools = {}
        self.name = name
        self.last = None  # last added PipelineTool or list of tools

    def add(self, tool, name=None):
        """Add a tool to the pipeline. The method returns the tool
        wrapper that can be used to get and set the tools parameters. If no
        name is specified explicitly, the tools name is used. A
        PipelineException is raised if a tool with that name already exists.

        Paramter
        --------
        tool - the tool to add
        name - optional name of the tool within the pipeline context
        """
        if name is None:
            import jip.tools
            if isinstance(tool, jip.tools.Tool):
                name = tool.name
            else:
                name = tool._name
        if name in self.tools:
            count = 2
            while name in self.tools:
                name = name + "." + str(count)
                count += 1
        if isinstance(tool, PipelineTool):
            wrapper = tool
            wrapper._pipeline = self
            wrapper._name = name
        else:
            wrapper = PipelineTool(self, tool, name)
        self.tools[name] = wrapper
        self.last = [wrapper]
        return wrapper

    def validate(self):
        """Validate all the steps in the pipeline and raises
        a PipelineException if case a step does not validate. The
        error contains a dict of dict where the key is the step
        name and the value is the validation error dictionary given
        by the step validation that contains the fields as keys and
        the error messages as values. For example:

            >>> try:
            >>>     pipeline.validate()
            >>> except PipelineException, e:
            >>>     for step, errs in e.validation_errors.items():
            >>>         for field, desc in errs.items():
            >>>             print "%s -> %s" % (field, desc)

        This will print the field names and error messages for each step
        that failed validation
        """
        errs = {}
        for step in self.tools.values():
            try:
                step.validate()
            except ValidationException, e:
                errs[step._name] = dict(e.errors)
        if len(errs) > 0:
            e = PipelineException("Pipeline validation failed!")
            e.validation_errors = errs
            raise e

    def get(self, name):
        """Lookup a pipeline tool by its name. The methods raises a
        PipelineException if the tool does not exist.

        Parameter
        ---------
        name - the name of the tools
        """
        if name not in self.tools:
            raise PipelineException("Tool %s not found!" % (name))
        return self.tools[name]

    def get_configuration(self, tool):
        """Resolve and return the configuration for a given tool. This
        returns a dictionary with the fully resolved configuration for a
        given tool.

        Paramter
        --------
        tool - the tool configuration
        """
        if tool is None:
            raise ValueError("No tool specified")
        if not isinstance(tool, PipelineTool):
            tool = self.tools[tool]
        return tool.get_configuration()

    def run(self):
        """Get the pipeline tools in order and execute them"""
        steps = self.get_sorted_tools()
        for i, step in enumerate(steps):
            if not step.is_done():
                step.run()

    def submit(self, grid):
        """Simple submission wrapper that sends this pipeline to the given
        cluster implementation and returns a list of jobs

        """
        steps = self.get_sorted_tools()
        features = []
        for i, step in enumerate(steps):
            if not step.is_done():
                features.append(grid.submit(step, step.get_configuration()))
        return features

    def get_sorted_tools(self):
        """Returns all tools in the pipeline in execution order. This does
        check for circular dependencies and raises a PipelineException if no
        valid execution order can be determined.
        """

        ret = []
        for s in self._topological_sort():
            ret.extend(list(s))
        return ret

    def _topological_sort(self):
        """Sort all tools in this pipeline by topological order
        based on the dependencies between the tools.
        """
        data = {}
        for n, t in self.tools.items():
            data[t] = t.get_dependencies()
        # Ignore self dependencies.
        for k, v in data.items():
            v.discard(k)
        # Find all items that don't depend on anything.
        reduce(set.union, data.itervalues()) - set(data.iterkeys())
        while True:
            ordered = set(item for item, dep in data.iteritems() if not dep)
            if not ordered:
                break
            yield ordered

            nd = {}
            for item, dep in data.iteritems():
                if item not in ordered:
                    nd[item] = (dep - ordered)
            data = nd

    @staticmethod
    def __find_path(spanning_tree, source, target):
        """
        Find a path from source to target by traversing the spanning tree
        """
        path = []
        while (source != target):
            if source is None:
                return []  # no path from source to target
            path.append(source)
            source = spanning_tree[source]
        path.append(source)
        path.reverse()
        return path

    def _detect_circles(self, node):
        """Detect a circle the contains the given node. If such circle exists,
        a CircularDependencyException is raised
        """
        visited = set()
        spanning_tree = {}
        cycles = []

        def dfs(node):
            visited.add(node)
            for child in node._out_edges:
                if child not in visited:
                    spanning_tree[child] = node
                    dfs(child)
                else:
                    if spanning_tree[node] != child:
                        cycle = Pipeline.__find_path(spanning_tree, node,
                                                     child)
                        if cycle:
                            cycles.append(cycle)

        spanning_tree[node] = None
        # Explore node's connected component
        dfs(node)
        if len(cycles) > 0:
            raise CircularDependencyException(cycles[0])

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        pass

    def __lshift__(self, tool):
        """Add a new tool or pipeline to this pipeline"""
        if isinstance(tool, Pipeline):
            for k, v in tool.tools.items():
                self.add(v)
        else:
            self.add(tool)
        return self

    def __rshift__(self, other):
        # same as or | pipe, but enforces a sequencial dependency
        # where streaming is not allowed and jobs will not be merged
        # to the same script
        last = self.last
        self.__or__(other)
        for s in last:
            for t in self.last:
                t._sequential.append(s)

    def __or__(self, tool):
        # or | pipe creates a dependency between last added tool
        # output and current tool input
        if not isinstance(tool, (list, tuple)):
            tool = [tool]

        last = self.last
        if not isinstance(last, (list, tuple)):
            last = [last]
        added = list([self.add(t) for t in tool])
        self.last = added

        if last is not None:
            # link the input/output
            #[last.get_dependencies().append(t) for t in tool]
            outputs = [l.get_default_output() for l in last]
            for t in added:
                out = outputs
                if len(out) == 0:
                    out = None
                if len(out) == 1:
                    out = out[0]

                t.__setattr__(t.get_default_input(), out)
        return self

    def __and__(self, tool):
        # resolve pipeline
        if isinstance(tool, Pipeline):
            tools = []
            for k, t in tool._tools.items():
                tools.append(t)
            tool = tools

        # make sure we have a list
        if not isinstance(tool, (list, tuple)):
            tool = [tool]
        # just add the tools
        added = list([self.add(t) for t in tool])
        self.last = added

    def __repr__(self):
        return self.name

    def to_json(self):
        """Convert the pipeline with configuration to JSON"""
        import json
        print json.dumps([p.to_json() for p in self.tools])


class PipelineTool(object):
    """ A pipeline tool is a wrapper around a given tool instance
    within a pipeline. The PipelineTool managed tool configuraiton and
    dependencies. Tool configurations are exposed as properties of this
    tool, but note that thay are not managed as raw values. If you add
    a configuration value, i.e:

        >>> p = Pipeline
        >>> pipeline_tool = p.add(MyTool())
        >>> pipeline_tool.input = "myfile.txt"
        >>> assert pipeline_tool.input != "myfile.txt"
        >>> assert pipeline_tool.input.raw() == "myfile.txt"

    The configuration values are wrapped in :class:Paramter instances in order
    to manage dependencies and resolve template baed values. For example:

        >>> pipeline_tool.name = "myname"
        >>> pipeline_tool.outfile = "${name}.txt"
        >>> assert pipeline_tool.outfile.get() == "myname.txt"

    Note that setting values will check for circular dependencies and might
    raise a CircularDependencyException in case a loop is detected.
    """
    def __init__(self, pipeline, tool, name):
        """Initialize a new PipelineTool

        :param pipeline: the pipeline
        :type pipeline: Pipeline
        :param tool: the tool instance
        :type tool: another.tools.Tool
        :param name: the name
        :type name: string
        """
        self._pipeline = pipeline
        self._tool = tool
        self._kwargs = {}
        self._name = name
        self._in_edges = set([])
        self._out_edges = set([])
        self._sequential = []
        if tool._default_configuration is not None:
            for k, v in tool._default_configuration.items():
                self.__setattr__(k, v)

    def get_dependencies(self):
        """Return a set of all dependencies of this intance"""
        deps = set([])
        for k, v in self._kwargs.items():
            if v.pipeline_tool != self:
                deps.add(v.pipeline_tool)
        return deps

    def run(self):
        """Run the underlying tool with the current configuration"""
        self._tool.run(self.get_configuration())

    def is_done(self):
        """Returns the tools is_done() value using the current configuration"""
        return self._tool.is_done(self.get_configuration())

    def validate(self):
        """Validate the tool using the current configuration"""
        self._tool.validate(self.get_configuration(),
                            self.get_incoming_configuration())

    def cleanup(self, failed=False):
        """Delegate to the tools cleanup method"""
        self._tool.cleanup(self.get_configuration(), failed=failed)

    def add_arguments(self, parser):
        self._tool.add_arguments(parser)

    def get_incoming_configuration(self):
        """Creates the configuration dictionary and but excludes entries
        that are set explicitly for this tool and includes only the fields
        that are set via an incoming dependency
        """
        cfg = {}
        # remove the job
        for name, value in self._kwargs.items():
            if value.pipeline_tool != self:
                cfg[name] = value.get()
        return cfg

    def get_configuration(self):
        """Return the fully resolved configuration for this tool"""
        config = dict(self._tool._get_default_configuration())
        for name, value in self._kwargs.items():
            config[name] = value.get()

        # resolve
        config.update(self._tool._resolve(config))
        config["job"] = self._tool.job
        return config

    def get_raw_configuration(self):
        """Return the rawm, unresolved configuration for this tool"""
        config = dict(self._tool._get_default_configuration())
        for k, v in self._kwargs.items():
            if v.pipeline_tool == self:
                config[k] = v.value
            else:
                config[k] = v.get()
        config["job"] = self._tool.job
        return config

    def __getattr__(self, name):
        if name in self._kwargs:
            return self._kwargs[name]
        else:
            if name == "job":
                return self._tool.job
            raw = self._tool._get_value(name)
            self._kwargs[name] = Parameter(self, name, raw)
            return self._kwargs[name]

    def __setattr__(self, name, value):
        if name not in ["_pipeline", "_tool", "_kwargs", "_name", "job",
                        "_in_edges", "_out_edges", "_sequential"]:
            if not isinstance(value, Parameter):
                self._kwargs[name] = Parameter(self, name, value)
            else:
                self._kwargs[name] = value
                # updated dependencies
                node = value.pipeline_tool
                # add edges
                if node != self:
                    self._in_edges.add(node)
                    node._out_edges.add(self)
                else:
                    # self loop
                    raise CircularDependencyException([self, self])
                self._pipeline._detect_circles(self)
        elif name == "job":
            # delegate to the tools job
            self._tool.job = value
        elif name == "threads":
            self._tool.job.threads = value
        else:
            object.__setattr__(self, name, value)

    def get_default_input(self):
        """Return the default input option"""
        for k, v in self._tool.inputs.items():
            return k

    def get_default_output(self):
        """Return the default input option"""
        for k, v in self._tool.outputs.items():
            return self.__getattr__(k)

    def __repr__(self):
        return self._name


class Parameter(object):
    """Wrapper class around PipelineTool configuration values
    in order to manage dependencies and resolve template values
    based on the context
    """
    def __init__(self, pipeline_tool, attr, value):
        self.pipeline_tool = pipeline_tool
        self.attr = attr
        self.value = value

    def get(self):
        """Ret the resolved value of the paramter"""
        raw = self.pipeline_tool.get_raw_configuration()
        resolved = self.pipeline_tool._tool._resolve(raw)
        return resolved[self.attr]

    def __repr__(self):
        return str(self.get())

    def __str__(self):
        return self.__repr__()
