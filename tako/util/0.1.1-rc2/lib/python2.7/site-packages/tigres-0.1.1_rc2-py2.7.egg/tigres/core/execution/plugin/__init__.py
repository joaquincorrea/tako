"""
`tigres.core.execution.plugin`
==============================

.. currentmodule:: tigres.core.execution.plugin

:platform: Unix, Mac
:synopsis: The core tigres execution plugin api

Classes
=======
 * :py:class:`ExecutionPluginBase` - Base class for Execution Plugins inherits from `types.ModuleType`.

.. moduleauthor:: Val Hendrix <vchendrix@lbl.gov>

"""
from tigres.core.utils import get_metaclass

__all__ = ['job', 'local', 'distribute']

from abc import abstractmethod, ABCMeta
import types


class ExecutionPluginBase(types.ModuleType,
                          get_metaclass(ABCMeta, 'ExecutionPluginBaseABCMeta')):
    """
    Base class for Execution Plugins inherits from `types.ModuleType`.
    """


    @classmethod
    def execute(cls, name, task, input_values, execution_data):
        if task.env:
            execution_data['env'].update(task.env)
        if 'FUNCTION' == task.task_type:
            return cls.execute_function(name, task, input_values,
                                        execution_data)
        else:
            return cls.execute_executable(name, task, input_values,
                                          execution_data)

    @classmethod
    @abstractmethod
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


        """
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def execute_executable(cls, name, task, input_values, execution_data):
        """ Abstract method for execution of the Task as an executable for the given input values on the local machine

        :param name: unique name for this execution
        :param task: the task for execution
        :type task: tigres.type.Task
        :param input_values: the input values for the task execution
        :type input_values: tigres.type.InputValues
        :returns: results and status
        :rtype : tuple
        """
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def parallel(cls, parallel_work, run_fn):
        """ Executes in parallel.
        Shared by parallel, split and merge templates

        :param parallel_work: The parallel state object to execute.
        :type parallel_work: tigres.core.state.WorkParallel
        :param run_fn: function to state state with, it should take a WorkUnit as input
        """
        raise NotImplementedError
