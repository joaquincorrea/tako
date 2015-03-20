"""
.. currentmodule:: tigres.core.template

:platform: Unix, Mac
:synopsis: The internal task api

`tigres.core.template`
**********************

The Tigres Template API contains template functions that are exposed to the
user-level Tigres API.


Functions
=========
 * :func:`merge` -  Collect output of parallel tasks
 * :func:`parallel` - List of tasks processing their inputs in parallel
 * :func:`sequence` - List of tasks placed in series; output of one is the input of the next
 * :func:`split` - Single 'split' task feeding inputs to a set of parallel tasks. The parallel task inputs must be explicitly defined either with PREVIOUS or explicit values


.. moduleauthor:: Val Hendrix <vhendrix@lbl.gov>

"""
LIST_ATTRIBUTES = [p for p in dir(list) if not p.startswith('_')]

from tigres.core.state.coordination import run_template_sequence, \
    run_template_parallel, run_template_split, \
    run_template_merge, get_results

get_results = get_results


def sequence(name, task_array, input_array, env=None):
    """ List of tasks placed in series; If no inputs are given for any task,
    the output of the previous task or template is the input of the next.

    Valid configurations of :code:`task_array` and :code:`input_array` is:

    *  :strong:`InputArray has zero or more InputValues and TaskArray has one or more tasks:`
       For each task that does not have an :class:`InputValues` the output of the previous
       task will be the input for the next task.


    :param name: an optional name can be assigned to the sequence
    :param task_array: an array containing tasks to execute
    :type task_array: TaskArray or list
    :param input_array: an array of input data for the specified tasks
    :type input_array: InputArray or list
    :param env: keyword arguments for the template task execution environment
    :type env: dict
    :return: results from the last task executed in the sequence. If the results of the last sequence
             is from a python function the output type will be defined by the  type returned otherwise
             executable output will be a string.
    :rtype: object or str


    :Example:

    >>> from tigres import *
    >>> def adder(a, b): return a + b
    >>> task1 = Task("Task 1", FUNCTION, adder)
    >>> task2 = Task("Task 2", EXECUTABLE, "echo")
    >>> tasks = TaskArray(None, [task1, task2])
    >>> inputs = InputArray(None, [InputValues(None, [2, 3]), InputValues(None, ['world'])])
    >>> sequence("My Sequence", tasks, inputs)
    'world'

    """

    if not name:
        name = sequence.__name__

    return run_template_sequence(name, task_array, input_array, env=env)


def parallel(name, task_array, input_array, env=None):
    """List of tasks processing their inputs in parallel.

    Valid configurations of :code:`task_array` and :code:`input_array` are:

    #. :strong:`TaskArray and InputArray are of equal length:`  Each :class:`InputValues` in the
       :class:`InputArray` will be match with the :class:`Task` at the same index in the
       :class:`TaskArray`
    #. :strong:`InputArray has zero or one InputValues and TaskArray has one or more tasks:`
       If there is one :class:`InputValues`, it will be reused for all tasks in the :class:`TaskArray`.
       If there are no :class:`InputValues` then the output from the previous task or template will
       be the input for each Task in the TaskArray.
    #. :strong:`TaskArray has one task and InputArray has one or more InputValues:`
       The one :class:`Task` will be executed with each :class:`InputValues` in the :class:`InputArray`

    .. note:: If the following conditions are met:
          ``a)`` no inputs are given to the parallel task (:code:`input_array`)
          ``b)`` the results of the previous template or task is iterable
          ``c)`` there is only one parallel task in the :code:`task_array`
          then each item of the iterable results becomes a parallel task.

    :param name: an optional name can be assigned to the sequence
    :type name: str or None
    :param task_array: an array containing tasks to execute
    :type task_array: TaskArray
    :param input_array: an array of input data for the specified tasks
    :type input_array: InputArray
    :param env: keyword arguments for the template task execution environment
    :return: list of task outputs
    :rtype: list

    :Example:

    >>> from tigres import *
    >>> def adder(a, b): return a + b
    >>> task1 = Task(None, FUNCTION, adder)
    >>> task2 = Task(None, EXECUTABLE, "/bin/echo")
    >>> inputs = InputArray(None,[InputValues("One",[1,2]), InputValues("Two",['world'])])
    >>> tasks = TaskArray(None,[task1, task2])
    >>> parallel("My Parallel", tasks, inputs)
    [3, 'world']
    """

    if not name:
        name = parallel.__name__

    return run_template_parallel(name, task_array, input_array, env=env)


def split(name, split_task, split_input_values, task_array, input_array,
          env=None):
    """ Single 'split' task feeding inputs to a set of parallel tasks. The parallel
    task inputs (:code:`input_array`) must be explicitly defined  with
    :class:`PREVIOUS` or explicit values.

    .. note:: If the following conditions are met:
            ``a)`` no inputs are given to the parallel task (:code:`input_array`)
            ``b)`` the results of the :code:`split_task` is iterable
            ``c)`` there is only one parallel task in the :code:`task_array`
            then each item in the iteration becomes a parallel task.

    :param name: Name of the split
    :type name: str
    :param split_task: first task to be run, followed by tasks on task_array
    :type split_task: Task
    :param split_input_values: input values for first task
    :type split_input_values: InputValues or list
    :param task_array: array of tasks to be run in parallel after first
    :type task_array: TaskArray
    :param input_array: array of input values for task array, default PREVIOUS
    :type input_array: InputArray or list
    :param env: keyword arguments for the template task execution environment
    :return: Output of the parallel operation
    :rtype: list(object)
    """

    if not name:
        name = split.__name__

    return run_template_split(name, split_task, split_input_values, task_array,
                              input_array, env=env)


def merge(name, task_array, input_array, merge_task, merge_input_values=None,
          env=None):
    """ Collect output of parallel tasks

    Single 'merge' task is being fed inputs from a set of parallel tasks. The parallel
    task inputs (:code:`input_array`) must be explicitly defined  with
    :class:`PREVIOUS` or explicit values.

    .. note:: If the following conditions are met:
          ``a)`` no inputs are given to the parallel task (:code:`input_array`)
          ``b)`` the results of the previous template or task is iterable
          ``c)``  there is only one parallel task in the :code:`task_array`
          then each item of the iterable results becomes a parallel task.

    :param name: The name of the merge
    :param task_array: array of tasks to be run in parallel before last task
    :type task_array: TaskArray
    :param input_array: array of input values for task array
    :type input_array: InputArray or list or None
    :param merge_task: last task to be run after all tasks on task array finish.
                       This task will receive a *list* of values, whose length is equal to the length of the
                       task array or the list .
    :type merge_task: Task
    :param merge_input_values: List of input values for last task, or the outputs of each task (as a list),
                               if None.
    :type merge_input_values: InputValues or None
    :param env: keyword arguments for the template task execution environment
    :return: results from the merge task. If the results of the merge task
             is from a python function the output type will be defined by the type returned otherwise
             executable output will be a string.

    :rtype: object or string
    """

    if not name:
        name = merge.__name__

    return run_template_merge(name, task_array, input_array, merge_task,
                              merge_input_values, env=env)

