#!/usr/bin/env python
"""Another tool pipeline impplementation
"""
from functools import reduce
from another.cluster import JobTemplate


class PipelineError(Exception):
    pass


class Pipeline(object):
    """The pipeline class wrapps around a list of tools
    and manages dependencies and paramters
    """
    def __init__(self):
        self.tools = {}

    def add(self, tool, name=None):
        """Add a tool to the pipeline. The method returns the tool
        wrapper that can be used to get and set the tools parameters.
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

    def _get_configuration(self, tool):
        """Resolve and return the configuration for the given tool"""
        if tool is None:
            raise ValueError("No tool specified")
        if not isinstance(tool, PipelineTool):
            tool = self.tools[tool]

        return tool._get_configuration()

    def get_sorted_tools(self):
        ret = []
        for s in self._topological_sort():
            ret.extend(list(s))
        return ret

    def _topological_sort(self):
        """Sort all tools in this pipeline by topological order
        based on the dependencies between the tools.
        """
        data = dict({t: t.get_dependencies() for n, t in self.tools.items()})
        # Ignore self dependencies.
        for k, v in data.items():
            v.discard(k)
        # Find all items that don't depend on anything.
        extra_items_in_deps = reduce(set.union,
                                     data.itervalues()) - set(data.iterkeys())
        # Add empty dependences where needed
        data.update({item: set() for item in extra_items_in_deps})
        while True:
            ordered = set(item for item, dep in data.iteritems() if not dep)
            if not ordered:
                break
            yield ordered
            data = {item: (dep - ordered)
                    for item, dep in data.iteritems() if item not in ordered}


class PipelineTool(object):
    def __init__(self, pipeline, tool, name):
        self._pipeline = pipeline
        self._tool = tool
        self._kwargs = {}
        self._name = name
        self.job = JobTemplate()

    def get_dependencies(self):
        deps = set([])
        for k, v in self._kwargs.items():
            if v.pipeline_tool != self:
                deps.add(v.pipeline_tool)
        return deps

    def _get_configuration(self):
        """Return the fully resolved configuration for this tool"""
        config = dict(self._tool._get_default_configuration())
        config.update(dict({name: value.get()
                            for name, value in self._kwargs.items()}))

        # resolve
        config.update(self._tool._resolve(config))
        return config

    def _get_raw_configuration(self):
        """Return the fully resolved configuration for this tool"""
        config = dict(self._tool._get_default_configuration())
        for k, v in self._kwargs.items():
            if v.pipeline_tool == self:
                config[k] = v.value
            else:
                config[k] = v.get()

        # resolve
        #config.update(self._tool._resolve(config))
        return config

    def __getattr__(self, name):
        if name in self._kwargs:
            return self._kwargs[name]
        else:
            raw = self._tool._get_value(name)
            self._kwargs[name] = Parameter(self, name, raw)
            return self._kwargs[name]

        raise AttributeError("%r object has no attribute %r" %
                             (type(self).__name__, name))

    def __setattr__(self, name, value):
        if name not in ["_pipeline", "_tool", "_kwargs", "_name"]:
            if not isinstance(value, Parameter):
                self._kwargs[name] = Parameter(self, name, value)
            else:
                self._kwargs[name] = value
        else:
            object.__setattr__(self, name, value)

    def __repr__(self):
        return self._name


class Parameter(object):
    def __init__(self, pipeline_tool, attr, value):
        self.pipeline_tool = pipeline_tool
        self.attr = attr
        self.value = value

    def get(self):
        raw = self.pipeline_tool._get_raw_configuration()
        resolved = self.pipeline_tool._tool._resolve(raw)
        return resolved[self.attr]
