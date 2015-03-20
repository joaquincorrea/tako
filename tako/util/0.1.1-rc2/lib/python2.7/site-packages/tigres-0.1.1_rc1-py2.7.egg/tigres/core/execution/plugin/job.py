"""
`tigres.core.execution.plugin.job`
========================

.. currentmodule:: tigres.core.execution.plugin.job

:platform: Unix, Mac
:synopsis: Execution Job Management plugin API


.. moduleauthor:: Val Hendrix <vchendrix@lbl.gov>

"""

from abc import abstractmethod

try:
    from subprocess import getstatusoutput
except ImportError:
    from commands import getstatusoutput
from copy import copy
import os
import re

try:
    from cloud import cloud

    pickle = cloud.serialization.cloudpickle
except ImportError:
    import pickle

from tigres.core.execution.plugin import ExecutionPluginBase
from tigres.core.execution.utils import create_executable_command, run_command
from tigres.utils import TigresException, State


class ExecutionPluginJobManagerBase(ExecutionPluginBase):
    """
    Base Class for Job Manager plugins. Inherits from `ExecutionPluginBase`.
    """
    FUNCTION_SCRIPT = """# -*- coding: utf-8 -*-
import pickle
fn = pickle.load(open('./{job_script_name}.code','rb'))
args = pickle.loads(pickle.load(open('./{job_script_name}.args','rb')))
try:
    result = fn(*args)
    pickle.dump(result, open('./{job_script_name}.result','wb'))
except Exception as e:
    result = None
    pickle.dump(result, open('./{job_script_name}.result','wb'))
    raise e"""

    @classmethod
    def execute_function(cls, name, task, input_values, execution_data):
        """ Executes the Task as a function for the given input values on the local machine

        :param task: the task for execution
        :type task: tigres.type.Task
        :param input_values: the input values for the task execution
        :type input_values: tigres.type.InputValues

        """
        if not execution_data or 'job_id' not in list(execution_data.keys()):
            # Create a script from the python function
            function_script_filename = cls.write_function_script(name,
                                                                 task.impl_name,
                                                                 input_values)

            cmd = "python {}".format(function_script_filename)

            # Build the Batch Job script
            job_script_name = cls.write_job_script(cmd, name)

            # Submit the batch job
            job_id = cls._submit_job(job_script_name, *execution_data['env'])
            execution_data['job_script_name'] = job_script_name
            execution_data['job_id'] = job_id

        # Determine if the job finishes
        if cls._is_job_finished(execution_data['job_id'],
                                execution_data['job_script_name']):
            error, output = cls._get_job_output(execution_data['job_id'],
                                                execution_data[
                                                    'job_script_name'],
                                                output_suffix='result')
            cls._clean_up_files(execution_data['job_script_name'])
            if not output and error:
                raise TigresException(error)

            # deserialize the pickled output
            output = pickle.loads(output)

            return output, State.DONE
        else:
            return None, State.RUN

    @classmethod
    def execute_executable(cls, name, task, input_values, execution_data):
        """ Executes the Task as an executable for the given input values on the local machine

        :param task: the task to execute
        :type task: tigres.type.Task
        :param input_values: the input values for the task
        :type input_values: InputValues or list

        """
        if not execution_data or 'job_id' not in list(execution_data.keys()):
            cmd = create_executable_command(input_values, task)

            # Build the Batch Job script
            job_script_name = cls.write_job_script(cmd, name)

            # Submit the batch job
            job_id = cls._submit_job(job_script_name, *execution_data['env'])
            execution_data['job_script_name'] = job_script_name
            execution_data['job_id'] = job_id

        # Determine if the job finishes
        if cls._is_job_finished(execution_data['job_id'],
                                execution_data['job_script_name']):
            error, output = cls._get_job_output(execution_data['job_id'],
                                                execution_data[
                                                    'job_script_name'])
            cls._clean_up_files(execution_data['job_script_name'])
            if not output and error:
                raise TigresException(error)

            return output, State.DONE
        else:
            return None, State.RUN

    @classmethod
    def parallel(cls, parallel_work, run_fn):
        """ Executes in parallel.
        Shared by parallel, split and merge templates

        :param run_fn:
        :param parallel_work:
        :return: dictionary of task outputs
        :rtype: dict, one list of output objects per task
        """
        copy_parallel_work = copy(parallel_work)
        while len(copy_parallel_work) > 0:

            for work in copy_parallel_work:
                # submit tasks with assoc. inputs
                run_fn(work)
                if work.state in (State.DONE, State.FAIL):
                    copy_parallel_work.remove(work)

    @classmethod
    def _clean_up_files(cls, job_script_name):
        """
        Remove all of the jobscript files
        :param job_script_name:
        :return:
        """
        pattern = "{}\..*".format(job_script_name)
        for root, dirs, files in os.walk('.'):
            for f in [x for x in files if re.match(pattern, x)]:
                os.remove(os.path.join(root, f))

    @classmethod
    @abstractmethod
    def _get_job_script(cls, *args, **kwargs):
        """
        Returns the job script content
        """
        return ""

    @classmethod
    def _get_job_output(cls, job_id, name, output_suffix='out'):
        """
        Returns the job output content
        """

        output_file_path = './{}.{}'.format(name, output_suffix)
        error_file_path = './{}.err'.format(name)
        with open(output_file_path) as output_file:
            output = output_file.read()

        with open(error_file_path) as error_file:
            error = error_file.read()
        return error.rstrip('\n'), output.rstrip('\n')

    @classmethod
    @abstractmethod
    def _is_job_finished(cls, job_id, job_script_name):
        """
        Determines if the requested job is finished
        """
        return job_id, job_script_name

    @classmethod
    @abstractmethod
    def _submit_job(cls, job_script_name, env=None):
        """
        Submits the specified job script
        """
        return job_script_name

    @classmethod
    def _create_job_script_name(cls, unique_name):
        job_script_name = "".join(x for x in unique_name if x.isalnum())
        return job_script_name


    @classmethod
    def write_function_script(cls, unique_name, task_impl, input_values):

        args = pickle.dumps(input_values)

        job_script_name = cls._create_job_script_name(unique_name)
        pickle.dump(task_impl, open("./{}.code".format(job_script_name), 'wb'))
        pickle.dump(args, open("./{}.args".format(job_script_name), 'wb'))
        job_script = cls.FUNCTION_SCRIPT.format(job_script_name=job_script_name)

        job_script_filename = "./{}.py".format(job_script_name)
        with open(job_script_filename, 'w') as f:
            f.write(job_script)

        return job_script_filename

    @classmethod
    def write_job_script(cls, cmd, unique_name):
        """

        :param cmd:  The command to call in the job script
        :param unique_name: The unique name for this job
        :return: the unique name of the job script (without file suffix)
        """
        job_script_name = cls._create_job_script_name(unique_name)
        job_script_filename = "./{}.sh".format(job_script_name)
        with open(job_script_filename, 'wb') as f:
            job_script = cls._get_job_script(command=cmd, name=job_script_name)
            f.write(job_script.encode('utf-8'))

        return job_script_name


class ExecutionPluginJobManagerSGE(ExecutionPluginJobManagerBase):
    """
    Plugin for Sun Grid Engine (SGE) execution
    """
    JOB_SCRIPT = """#!/bin/bash
#$ -cwd
#$ -S /bin/bash
#$ -j y	         # Combine stderr and stdout
#$ -o $JOB_NAME.$JOB_ID.out        # Name of the output file
#$ -V

{command} 2>{name}.err 1>{name}.out
"""

    @classmethod
    def _get_job_script(cls, **kwargs):
        """

        :param command:
        :return: job string with the command appended
        """

        return ExecutionPluginJobManagerSGE.JOB_SCRIPT.format(**kwargs)


    @classmethod
    def _submit_job(cls, job_script_name, env=None):
        """
        :param job_script_name:
        :return: the job manager job id
        """
        # Create the qsub command for submitting the job
        job_script_filename = "./{}.sh".format(job_script_name)
        qsub_command = "qsub -N {} {} ".format(job_script_name,
                                               job_script_filename)

        output = run_command(qsub_command, env=env)
        # get the job id
        job_id = output.split()[2]
        return job_id

    @classmethod
    def _is_job_finished(cls, job_id, job_script_name):
        check_command = "qstat -s z | grep {} | grep {} | grep -v ' qw '".format(
            job_script_name[:10], job_id)
        _, output = getstatusoutput(check_command)
        output = output.strip()
        job_finished = not not output

        if not job_finished:
            check_command = "qstat | grep {} | grep {}".format(
                job_script_name[:10], job_id)
            _, output = getstatusoutput(check_command)
            job_finished = not output

        return job_finished


class ExecutionPluginJobManagerSLURM(ExecutionPluginJobManagerBase):
    """ Plugin for SLURM execution

    """
    # TODO figure out how to pass in batch job specific args
    JOB_SCRIPT = """#!/bin/bash
#
#SBATCH --output={name}.%J.out

## Run command
{command} 2>{name}.err 1>{name}.out
"""

    @classmethod
    def _get_job_script(cls, **kwargs):
        """

        :param command:
        :return: job string with the command appended
        """

        return ExecutionPluginJobManagerSLURM.JOB_SCRIPT.format(**kwargs)

    @classmethod
    def _get_job_output(cls, job_id, name, output_suffix='out'):
        """
        Get the error and standard output for the job and then
        removes them.

        :param job_id:
        :param name:
        :return:
        """
        # Job is done. Get the output (replace new lines with spaces)
        check_error = "scontrol show job {} | grep JobState ".format(job_id)
        _, error_message = getstatusoutput(check_error)

        error, output = ExecutionPluginJobManagerBase._get_job_output(job_id,
                                                                      name,
                                                                      output_suffix)

        if not output and error:
            error = error_message + " " + error
        return error, output

    @classmethod
    def _submit_job(cls, job_script_name, env=None):
        """

        :param job_script_name:
        :return:
        """
        # Create the qsub command for submitting the job
        job_script_filename = "./{}.sh".format(job_script_name)
        qsub_command = "sbatch -J {} {} ".format(job_script_name,
                                                 job_script_filename)

        output = run_command(qsub_command, env=env)
        #get the job id
        job_id = output.split()[3]
        return job_id

    @classmethod
    def _is_job_finished(cls, job_id, job_script_name):
        check_command = "squeue -j {} | grep -v JOBID".format(job_id)
        _, output = getstatusoutput(check_command)
        output = output.strip()
        return not output
