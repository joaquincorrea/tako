"""
`tigres.core.execution.plugin.local`
========================

.. currentmodule:: tigres.core.execution.plugin.local

:platform: Unix, Mac
:synopsis: Execution Local plugin API


.. moduleauthor:: Val Hendrix <vchendrix@lbl.gov>

"""
from copy import copy
import multiprocessing
import os

from tigres.core.execution.utils import TaskThreadExecution, \
    TaskProcessExecution
from tigres.core.execution.plugin import ExecutionPluginBase
from tigres.core.execution.utils import create_executable_command, run_command
from tigres.utils import TigresException, State


class ExecutionPluginLocalBase(ExecutionPluginBase):
    """
    Plugin for local (threaded) Tigres Execution

    """

    @classmethod
    def execute_function(cls, name, task, input_values, execution_data):
        """ Executes the Task as a function for the given input values on the local machine

        :param name: unique name for this execution
        :param execution_data:
        :param task: the task for execution
        :type task: tigres.type.Task
        :param input_values: the input values for the task execution
        :type input_values: tigres.type.InputValues
        :returns: results and status
        :rtype : tuple


        :Example:

        >>> from tigres.utils import Execution
        >>> from tigres.core.execution import load_plugin
        >>> engine = load_plugin(Execution.LOCAL_THREAD)
        >>> from tigres import InputTypes, InputValues, Task, FUNCTION
        >>> def my_impl(a, b): return "foo {} {}".format(a, b)
        >>> input_type_1 = InputTypes(None, [int, str])
        >>> task1 = Task(None, FUNCTION, my_impl, input_type_1)
        >>> input_values_1 = InputValues(None, [1, 'hello'])
        >>> engine.execute_function("foo bar", task1, input_values_1,{'env':{}})
        ('foo 1 hello', 'DONE')

        """
        if not execution_data or State.DONE not in list(execution_data.keys()):
            # We copy the python environment and save the old one
            copy_env = copy(os.environ)
            old_env = os.environ
            try:
                # Prepare copied Environment
                os.environ = copy_env
                os.environ.update(execution_data['env'])

                results = task.impl_name(*input_values)
                if execution_data is not None:
                    execution_data[State.DONE] = State.DONE

                # Return environment back to its original state
                os.environ = old_env
                del copy_env
                return results, State.DONE
            except Exception as err:
                # Return environment back to its original state
                os.environ = old_env
                del copy_env
                raise TigresException(
                    "Exception caught for execution '{w}', Task '{t}'. Error: {e}".format(
                        w=name, t=task.name, e=err))

    @classmethod
    def execute_executable(cls, name, task, input_values, execution_data):
        """ Executes the Task as an executable for the given input values on the local machine

        :param task: the task to execute
        :type task: tigres.type.Task
        :param input_values: the input values for the task
        :type input_values: InputValues or list


        :Example:

        >>> from tigres.utils import Execution
        >>> from tigres.core.execution import load_plugin
        >>> engine = load_plugin(Execution.LOCAL_THREAD)
        >>> from tigres import InputTypes, InputValues, Task, EXECUTABLE
        >>> input_type_1 = InputTypes(None, [str])
        >>> task1 = Task(None, EXECUTABLE, "/bin/echo", input_type_1)
        >>> input_values_1 = InputValues(None, ['world'])
        >>> engine.execute_executable("foo bar", task1, input_values_1,{'env':{}})
        ('world', 'DONE')

        .. note::
            The current implementation will coerce all arguments to strings when it builds the
            command for command line execution
        """
        output = None

        if not execution_data or State.DONE not in list(execution_data.keys()):
            cmd = create_executable_command(input_values, task)
            # Get the current environment and pass it along to the executable
            copy_os_env = copy(os.environ)
            try:
                copy_os_env.update(execution_data['env'])
                output = run_command(cmd, env=copy_os_env)
                if execution_data is not None:
                    execution_data[State.DONE] = State.DONE
            except Exception as err:
                raise TigresException(
                    "Exception caught for execution '{w}', Task '{t}'. Error: {e}".format(
                        w=name, t=task.name, e=err))

        return output, State.DONE


class ExecutionPluginLocalProcess(ExecutionPluginLocalBase):
    """
    Plugin for local (threaded) Tigres Execution

    """


    @classmethod
    def parallel(cls, parallel_work, run_fn):
        """ Executes in parallel.
        Shared by parallel, split and merge templates

        :param parallel_work: The parallel state object to execute.
        :type parallel_work: tigres.core.state.WorkParallel
        :param run_fn: function to state state with, it should take a WorkUnit as input
        """
        # start threads
        task_processes = TaskProcessExecution(cls, parallel_work)
        task_processes.join()


class ExecutionPluginLocalThread(ExecutionPluginLocalBase):
    """
    Plugin for local (threaded) Tigres Execution

    """

    @classmethod
    def parallel(cls, parallel_work, run_fn):
        """ Executes in parallel.
        Shared by parallel, split and merge templates

        :param parallel_work: The parallel state object to execute.
        :type parallel_work: tigres.core.state.WorkParallel
        :param run_fn: function to state state with, it should take a WorkUnit as input
        """
        # start threads
        thread_count = multiprocessing.cpu_count()
        if thread_count > len(parallel_work):
            # there is no reason to have more threads than state
            thread_count = len(parallel_work)
        task_threads = TaskThreadExecution(run_fn, thread_count)
        # submit tasks with assoc. inputs
        for work in parallel_work:
            task_threads.submit(work)
            # wait for tasks to finish
        task_threads.join()
