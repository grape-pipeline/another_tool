#!/usr/bin/env python
"""Another tool pipeline impplementation to create pipelines of tools.
"""
from functools import reduce
from another.cluster import JobTemplate


class PipelineError(Exception):
    """Common error message raised by pipelines"""
    pass


class Pipeline(object):
    """The pipeline class wrapps around a list of tools
    and manages dependencies and paramters. You can add tools
    to the pipeline using the add() method. This returns a new
    :class:PipelineTool instance that can be used to configure the tool
    and create dependencies. For example:

        >>> pipeline = Pipeline()
        >>> a = pipeline.add(MyTool(), "a")
        >>> a.input = "myfile.txt"

    Here we creat a new pipeline and add a tool we call "a". We specify
    the name explicitly here, otherwise the tools name is used. Then
    we assign the `input` value for the tool. The name is important here
    as it can be used to lookup tools in the pipeline:

        >>> b = pipeline.add("a")
        >>> assert a == b

    """
    def __init__(self, name=None):
        self.tools = {}
        self.name = name

    def add(self, tool, name=None):
        """Add a tool to the pipeline. The method returns the tool
        wrapper that can be used to get and set the tools parameters. If no
        name is specified explicitly, the tools name is used. A PipelineError
        is raised if a tool with that name already exists.

        Paramter
        --------
        tool - the tool to add
        name - optional name of the tool within the pipeline context
        """
        if name is None:
            name = tool.name
        if name in self.tools:
            raise PipelineError("A tool with the name %s already exists in the"
                                " pipeline. Please specify the name explicitly"
                                " when calling add()!")

        wrapper = PipelineTool(self, tool, name)
        self.tools[name] = wrapper
        return wrapper

    def get(self, name):
        """Lookup a pipeline tool by its name. The methods raises a
        PipelineError if the tool does not exist.

        Parameter
        ---------
        name - the name of the tools
        """
        if name not in self.tools:
            raise PipelineError("Tool %s not found!" % (name))
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

    def get_sorted_tools(self):
        """Returns all tools in the pipeline in execution order. This does
        check for circular dependencies and raises a PipelineError if no
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
    """
    def __init__(self, pipeline, tool, name):
        self._pipeline = pipeline
        self._tool = tool
        self._kwargs = {}
        self._name = name

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
        self._tool.validate(self.get_configuration())

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

        raise AttributeError("%r object has no attribute %r" %
                             (type(self).__name__, name))

    def __setattr__(self, name, value):
        if name not in ["_pipeline", "_tool", "_kwargs", "_name", "job"]:
            if not isinstance(value, Parameter):
                self._kwargs[name] = Parameter(self, name, value)
            else:
                self._kwargs[name] = value
        elif name == "job":
            # delegate to the tools job
            self._tool.job = value
        else:
            object.__setattr__(self, name, value)

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
