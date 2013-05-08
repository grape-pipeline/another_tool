#!/usr/bin/env python
"""The another tool library provides the necessary infrastructure to submit
tool instances to a remote execution cluster. All the methods to implement
the cluster integration are contained in this module.

HPC cluster integration works in a rather simple way. The tool instance is
pickled and base64 encoded before it it wrapped in in a bash script. The
script is then submitted to the HPC grid. When the job starts executing,
the script will start a python interpreter, unpickle the tool instance and
run it.

If an exception is thrown during execution of the tool, the cluster job exits
with a non-zero exit code, which indicates the failure.

Results of a tool run can be loaded. Their result is pickled to stdout and
the cluster module provided the ability to load results from the log file. In
case an exception was raised, the exception is loaded and re-raised.
Otherwise the jobs return values is returned. This functionality is provided
by the :py:class:another.cluster.Feature class. An instance of a feture is
returned at job submission.
"""
import logging
import subprocess
import os
import sys
import time
from mako.template import Template
from another.tools import Tool
from another.pipelines import PipelineTool
import cPickle

# result separator
_SEP_RESULT = "-------------------RESULT-------------------"
_SEP_RESULT_END = "-------------------END-RESULT-------------------"

# the default job template
DEFAULT_TEMPLATE = """#!/bin/bash
#
# Auto-generated script that runs
# the tool using the another_tool
# library
#
${header or ''}

${script}
"""


class ClusterException(Exception):
    """Default error returned on submission failure"""
    pass


class _ToolWrapper(object):
    """Internal class that wraps the tool and its arguments and is pickled
    and send to the cluster. The instance is then un-pickled and runs the
    tool when the job is executed on the cluster.
    """

    def __init__(self, tool, args):
        """Initialize the wrapper with the tool and its arguments

        :param tool: the tool instance
        :param args: the tool arguments
        """
        self.tool = tool
        self.args = args

    def run(self):
        """Run the tool and catch any exception raised by the tool
        execution. If an exception is raised, it is logged and then
        returned as results. This allows the :py:class:Feature to pick
        up any exceptions raised in remote execution.
        """
        try:
            result = self.tool.run(self.args)
            return result
        except Exception, e:
            sys.stderr.write("Error while executing job: %s\n" % str(e))
            return e


class Feature(object):
    """Job feature returned by a cluster after submitting a job.
    The feature stores a references to the remote jobid and the
    jobs stdout and stderr files.
    It provides the ability to wait for a job as well as to fetch the jobs
    results.
    """
    def __init__(self, jobid, stdout=None, stderr=None):
        """Initialize a new Feature instance.

        :param jobid: the job id on the cluster
        :param stdout: the jobs stdout file
        :param stderr: the jobs stderr file
        """
        self.jobid = jobid
        self.stdout = stdout
        self.stderr = stderr

    def get(self, cluster, check_interval=360):
        """Wait until the job is finished and returns the result of the job.

        :param cluster: the cluster instance
        :param check_interval: the interval in which the job status is polled
        """
        self.wait(cluster, check_interval=check_interval)
        # try to load the result from stdout file
        result = self._load_results(self.stdout)
        if isinstance(result, Exception):
            raise result
        return result

    def _load_results(self, results):
        """Internal method to load the results
        from the given results file and return them
        """
        try:
            with open(results, 'r') as result_file:
                lines = None
                # get lines after separator line
                for line in result_file:
                    if lines is not None:
                        if line == _SEP_RESULT_END:
                            break
                        lines.append(line)
                    elif line.strip() == _SEP_RESULT:
                        lines = []
                return cPickle.loads("".join(lines).decode('base64'))
        except Exception, e:
            log = logging.getLogger("%s.%s" % (self.__module__,
                                                    self.__class__.__name__))
            log.error("Unable to load results from feature: %s", str(e))
            raise e

    def wait(self, cluster, check_interval=360):
        """Blocks until the jobs disappears from the cluster. No checks
        are made for success or failure state.

        :param cluster: the cluster instance
        :param check_interval: the interval in which the job status is polled
        """
        # wait for the job to disappear from the list
        cluster.wait(self.jobid, check_interval=check_interval)

    def cancel(self, cluster):
        """Cancel the job based on the jobid

        :param cluster: the cluster instance
        """
        pass

    def get_status(self, cluster):
        """Check the state of the job on the given remote cluster

        :param cluster: the cluster instance
        """
        pass


class Cluster(object):
    """The abstract base class for cluster implementation consists of a single
    method that is able to submit a tool to a compute cluster. The
    tool is passed as a dump string, usually created by calling
    dumps().
    The cluster implementation should interpret this string as valid bash,
    create the environment appropriately, and submit the job.

    The submission template must be further customizable by the user, where the
    following variables are allowed

     * header -- custom header that will be rendered into the template
     * script -- the tool_script

    The template is rendered using the mako library. The simplest use case
    is that variables expressed like ${script} are replaced by the template
    engine.

    In case the submission failed, a ClusterException is raised.
    """
    STATE_QUEUED = "Queued"
    STATE_RUNNING = "Running"
    STATE_DONE = "Done"
    STATE_FAILED = "Failed"


    def list(self):
        """A map of all active jobs on the cluster from the job id to the state
        """
        pass

    def submit(self, tool, args=None):
        """Submit the tool by wrapping it into the template
        and sending it to the cluster. If the tool is a string, given args
        are ignored and the script string is added as is into the template.
        If the tool is an instance of Tool, the tools dump method is
        used to create the executable script.

        This method return the job id associated with the job by the
        unterlying grid engine.

        Parameter
        ---------
        tool -- the Tool or PipelineTool instance to submit
        args -- tuple of *args and **kwargs that are passed to the tool dump
                in case the tool has to be converted to a script
        """
        template = tool.job.template
        if template is None:
            template = DEFAULT_TEMPLATE

        # collect dependencies
        # dependencies are resolved if the tool is
        # of class Tool or PipelineTool.
        deps = None
        tool_script = tool

        if isinstance(tool, Tool):
            # if a tool list passed, make it
            # an executable script
            tool_script = self._dump_tool(tool, args)
            deps = tool.job.dependencies
        elif isinstance(tool, PipelineTool):
            tool_script = self._dump_tool(tool._tool, args)
            # update dependencies
            deps = [str(d.job.jobid)
                    for d in filter(lambda t: t.job.jobid is not None,
                                    tool.get_dependencies())]
        if len(deps) == 0:
            deps = None

        # check and create log directory
        if tool.job.logdir is not None and not os.path.exists(tool.job.logdir):
            os.makedirs(tool.job.logdir)
        # render the job script
        rendered_template = Template(template).render(script=tool_script,
                                                      max_time=tool.job.max_time,
                                                      max_mem=tool.job.max_mem,
                                                      threads=tool.job.threads,
                                                      tasks=tool.job.tasks,
                                                      queue=tool.job.queue,
                                                      header=tool.job.header,
                                                      priority=tool.job.priority)
        # submit
        feature = self._submit(rendered_template,
                               name=tool.job.name,
                               max_time=tool.job.max_time,
                               max_mem=tool.job.max_mem,
                               threads=tool.job.threads,
                               tasks=tool.job.tasks,
                               queue=tool.job.queue,
                               priority=tool.job.priority,
                               dependencies=deps,
                               working_dir=tool.job.working_dir,
                               extra=tool.job.extra,
                               logdir=tool.job.logdir)

        # set the tools jobid
        tool.job.jobid = feature.jobid
        return feature

    def wait(self, jobid, check_interval=360):
        """Block until the job is no longer in any of the cluster queues.

        Paramter
        --------
        jobid -- the job id
        check_interval -- interval in second in which the job state should be
                          checked. If this is used depends on the cluster and
                          the implementations. If there is a better way than
                          polling the state in regular intervals, that should
                          be used in favor of the polling strategy
        """
        raise ClusterException("Wait is not implemented!")

    def dump(self, tool, args):
        """Save the given tool instance and the arguments and returns a string
        that is a valid bash script that will load and execute the tool.

        Note that the script does not set up the python environment or
        the paths. This has to be done by the script caller!

        Parameter
        ---------

        tool -- the tool that will be prepared for execution
        args -- tool arguments

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
print "%s"
print result_string
print "%s"
if isinstance(result, Exception):
    sys.exit(1)
'<< __EOF__
%s__EOF__

"""
        wrapper = _ToolWrapper(tool, args)
        return template % (_SEP_RESULT, _SEP_RESULT_END,
                           cPickle.dumps(wrapper).encode("base64"))

    def _dump_tool(self, tool, args):
        """Dump a tool instance to an executable script
        using the args and kwargs in the args paramters)
        """
        #dump the tool with arguments
        return self.dump(tool, args)

    def _submit(self, script, max_time=0, name=None,
                max_mem=0, threads=1, queue=None, priority=None, tasks=1,
                dependencies=None, working_dir=None, extra=None, logdir=None):
        """This method must be implemented by the subclass and
        submit the given script to the cluster. Please note that
        the script is passed as a string. It depends on the implementation
        and the grid engine if this script is supposed to be written to disk
        or can be submitted by other means.

        Parameter
        ---------
        tool_script -- the fully rendered script string
        max_time -- the maximum wallclock time of the job in seconds
        name     -- the name of the job
        max_mem  -- the maximum memory that can be allocated by the job
        threads  -- the number of cpus slots per task that should be allocated
        tasks    -- the number of tasks executed by the job
        queue    -- the queue the ob should be submitted to
        prority  -- the jobs priority
        dependencies -- list or string of job ids that this job depends on
        extra    -- list of any extra parameters that should be considered
        logdir   -- base log directory
        """
        pass

    def _add_parameter(self, params, name=None, value=None, exclude_if=None,
                       to_list=None, prefix=None):
        """This is a helper function to create paramter arrays that are passed
        to subprocess.Popen.
        If name is not none the pair (name, value) is added
        to the params list, otherwise just value is appended as long as
        the value is not a list or a tuple. Otherwise the params array is
        extended by the list/tuple.
        If valu is None noting is added to the params list. In addition
        you can specify the exclude_if fundtion. If the function is specified,
        the value is only added if the function returns False.
        Lists of values will be joined to a single string value if
        to_list is specified. The value of to_list is used to join the list
        elements.

        Paramter
        --------
        params -- the target list
        name   -- name of the paramter
        value  -- the raw value
        exclude_if -- function that should return True if the value shoudl be
                      excluded
        to_list -- if specified and the value is a list, the list is joined to
                   a string using the to_list value
        prefix  -- prefix the value before ading it to the paramter list
        """
        if value is None:
            return
        if exclude_if is not None and exclude_if(value):
            return

        # eventually convert a list value to a string
        value = self.__check_list(value, to_list)

        if prefix is not None:
            value = "%s%s" % (prefix, str(value))

        if name is not None:
            params.extend([str(name), str(value)])
        else:
            if isinstance(value, (list, tuple,)):
                params.extend(value)
            else:
                params.append(str(value))

    def __check_list(self, value, to_list):
        if to_list is not None:
            if not isinstance(value, (list, tuple,)):
                value = [value]
            value = to_list.join(value)
        return value

    def log(self):
        """Get the tool logger"""
        return logging.getLogger("%s.%s" % (self.__module__,
                                            self.__class__.__name__))


class Slurm(Cluster):
    """Slurm extension of the Cluster implementationcPickle.load(""

    The slurm implementation sends jobs to the cluster using
    the `sbatch` command line tool. The job parameter are paseed
    to `sbatch` as they are. Note that:

    * max_mem is passed as --mem-per-cpu

    """

    def __init__(self, sbatch="sbatch", squeue="squeue", list_args=None):
        """Initialize the slurm cluster.

        Paramter
        --------
        sbatch -- path to the sbatch command. Defaults to 'sbatch'
        squeue -- path to the squeue command. Defaults to 'squeue'
        """
        self.sbatch = sbatch
        self.squeue = squeue
        self.list_args = list_args

    def list(self):
        jobs = {}
        params = [self.squeue, "-h", "-o", "%i,%t"]
        if self.list_args is not None:
            params.extend(self.list_args)

        process = subprocess.Popen(params,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE,
                                   shell=False)
        for l in process.stdout:
            jid, state = l.strip().split(",")
            js = Cluster.STATE_QUEUED
            if state == "R":
                js = Cluster.STATE_RUNNING
            jobs[jid] = js
        err = "".join([l for l in process.stderr])
        if process.wait() != 0:
            raise ClusterException("Error while submitting job:\n%s" % (err))
        return jobs

    def _submit(self, script, max_time=None, name=None,
                max_mem=None, threads=1, queue=None, priority=None, tasks=1,
                dependencies=None, working_dir=None, extra=None, logdir=None):
        params = [self.sbatch]

        if logdir is None:
            logdir = os.getcwd()
        logdir = os.path.abspath(logdir)

        stdout_file = os.path.join(logdir, "slurm-%j.out")
        stderr_file = os.path.join(logdir, "slurm-%j.err")



        self._add_parameter(params, "-t", max_time,
                            lambda x: x is None or int(x) <= 0)
        self._add_parameter(params, "-p", queue)
        self._add_parameter(params, "--qos", priority)
        self._add_parameter(params, "-c", threads,
                            lambda x: x is None or int(x) <= 0)
        self._add_parameter(params, "--mem-per-cpu", max_mem,
                            lambda x: x is None or int(x) <= 0)
        self._add_parameter(params, "-D", working_dir)
        self._add_parameter(params, "-d", dependencies, prefix="afterok:",
                            to_list=":")
        self._add_parameter(params, "-d", dependencies, prefix="afterok:",
                            to_list=":")
        self._add_parameter(params, "-J", name)
        self._add_parameter(params, "-e", stderr_file)
        self._add_parameter(params, "-o", stdout_file)
        self._add_parameter(params, value=extra)

        process = subprocess.Popen(params,
                                   stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE,
                                   shell=False)

        process.stdin.write(script)
        process.stdin.close()
        out = "".join([l for l in process.stdout])
        err = "".join([l for l in process.stderr])
        if process.wait() != 0:
            raise ClusterException("Error while submitting job:\n%s" % (err))
        job_id = out.strip().split(" ")[3]

        # calculate the full name to the log files
        stdout_file = os.path.join(logdir, "slurm-%s.out" % job_id)
        stderr_file = os.path.join(logdir, "slurm-%s.err" % job_id)

        feature = Feature(jobid=job_id, stdout=stdout_file, stderr=stderr_file)
        return feature

    def wait(self, jobid, check_interval=360):
        if jobid is None:
            raise ClusterException("No job id specified! Unable to check"
                                   "  job state!")

        while True:
            process = subprocess.Popen([self.squeue, '-h', '-j', str(jobid)],
                                       stderr=subprocess.PIPE,
                                       stdout=subprocess.PIPE)
            (out, err) = process.communicate()
            if process.wait() != 0 or len(out.strip()) == 0:
                return
            else:
                time.sleep(check_interval)

class SunGrid(Cluster):
    """SGE extension of the Cluster implementation

    The SGE implementation sends jobs to the cluster using
    the `qsub` command line tool. The job parameter are paseed
    to `qsub` as they are. Note that:
    """

    def __init__(self, qsub="qsub", qstat="qstat", list_args=None):
        """Initialize the SGE cluster.

        Parameter
        --------
        qsub -- path to the qsub command. Defaults to 'qsub'
        qstat -- path to the qstat command. Defaults to 'qstat'
        """
        self.qsub = qsub
        self.qstat = qstat
        self.list_args = list_args

    def list(self):
        jobs = {}
        params = [self.qstat, "-u", os.getenv('USER')]
        if self.list_args is not None:
            params.extend(self.list_args)

        process = subprocess.Popen(params,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE,
                                   shell=False)
        for l in process.stdout:
            fields = l.strip().split("\t")
            js = Cluster.STATE_QUEUED
            if len(fields) > 4 and fields[4] == "r":
                js = Cluster.STATE_RUNNING
            jobs[fields[0]] = js
        err = "".join([l for l in process.stderr])
        if process.wait() != 0:
            raise ClusterException("Error while submitting job:\n%s" % (err))
        return jobs

    def _submit(self, script, max_time=None, name=None,
                max_mem=None, threads=1, queue=None, priority=None, tasks=1,
                dependencies=None, working_dir=None, extra=None, logdir=None):
        params = [self.qsub]

        if logdir is None:
            logdir = os.getcwd()
        logdir = os.path.abspath(logdir)

        if working_dir is None:
            working_dir = os.path.abspath(os.getcwd())

        list = ["h_rt=%s" % max_time, "virtual_free=%s" % max_mem]
        self._add_parameter(params, "-q", queue)
        self._add_parameter(params, None, ['-pe', 'smp', str(threads)],
                            lambda x: x[2] == None or int(x[2] <= 0))
        self._add_parameter(params, "-N", name)
        self._add_parameter(params, "-now", "n")
        self._add_parameter(params, '-l',['h_rt', str(self._parse_time(max_time))],
                lambda x: x[1] == 'None' or int(x[1]) <= 0, to_list="=")
        self._add_parameter(params, '-l', ['virtual_free', str(max_mem)],
                lambda x: x[1] == 'None' or int(x[1]) <= 0, to_list="=")
        self._add_parameter(params, "-wd", working_dir,
                            lambda x: not os.path.exists(str(x)))
        self._add_parameter(params, "-hold_jid", dependencies,
                            to_list=",")
        self._add_parameter(params, "-e", logdir)
        self._add_parameter(params, "-o", logdir)
        self._add_parameter(params, value=extra)

        process = subprocess.Popen(params,
                                   stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE,
                                   shell=False)

        process.stdin.write(script)
        process.stdin.close()
        out = "".join([l for l in process.stdout])
        err = "".join([l for l in process.stderr])
        if process.wait() != 0:
            raise ClusterException("Error while submitting job:\n%s" % (err))
        import re
        expr = 'Your job (?P<job_id>.+) .+ has been submitted'
        match = re.search(expr, out)
        job_id = match.group('job_id')

        # calculate the full name to the log files
        stdout_file = os.path.join(logdir, "%s.o%s" % (name, job_id))
        stderr_file = os.path.join(logdir, "%s.e%s" % (name, job_id))

        feature = Feature(jobid=job_id, stdout=stdout_file, stderr=stderr_file)
        return feature

    def wait(self, jobid, check_interval=360):
        if jobid is None:
            raise ClusterException("No job id specified! Unable to check"
                                   "  job state!")

        while True:
            process = subprocess.Popen([self.qstat, '-j', str(jobid)],
                                       stderr=subprocess.PIPE,
                                       stdout=subprocess.PIPE)
            (out, err) = process.communicate()
            if process.wait() != 0 or len(out.strip()) == 0:
                return
            else:
                time.sleep(check_interval)

    def _parse_time(self, time):
        if time is None:
            return time
        t = map(lambda x: x or '0', time.split(':'))
        if len(t) is 1:
            return time
        if len(t) is not 3:
            raise ValueError('SunGrid: Invalid time string format')
        return int(t[0])*3600 + int(t[1])*60 + int(t[2])

