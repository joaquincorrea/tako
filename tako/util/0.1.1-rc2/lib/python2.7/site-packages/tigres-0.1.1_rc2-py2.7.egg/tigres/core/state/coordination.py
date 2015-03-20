"""
****************************
`tigres.core.state.coordination`
****************************

.. currentmodule:: tigres.core.state.coordination

:platform: Unix, Mac
:synopsis: The core api for coordinating work

Functions
=========

Work
----
 * :func:`prepare_work` - Prepares a :class:`WorkUnit` for execution
 * :func:`run_work` - Executes a :class:`WorkUnit`
 * :func:`run_work_sequence` - Executes a :class:`WorkSequence`
 * :func:`run_work_parallel` - Executes a :class:`WorkParallel`

Template
--------
 * :func:`run_template_sequence` - Runs a :class:`WorkSequence` which is an ordered list of :class:`WorkUnit`
 * :func:`run_template_parallel` - Runs a :class:`WorkParallel` which is an ordered list of :class:`WorkUnit`
 * :func:`run_template_split` - Runs :func:`split` template which consists of a :class:`WorkUnit` then a :class:`WorkParallel`
 * :func:`run_template_merge` - Runs :func:`merge` template which consists of a  :class:`WorkParallel` then a :class:`WorkUnit`

PREVIOUS syntax
---------------
 * :func:`evaluate_implicit_previous` -
 * :func:`resolve_previous_syntax` -

.. moduleauthor:: Val Hendrix <vhendrix@lbl.gov>


"""
import functools
from tigres.core.monitoring.common import Level, Keyword
from tigres.core.task import Task, TaskArray, validate_tigres_object, \
    InputArray, InputValues

try:
    xrange
    range = xrange
except NameError:
    pass

from copy import copy

try:
    from itertools import zip_longest
except ImportError:
    from itertools import izip_longest

    zip_longest = izip_longest
from tigres.core.state.program import Program
import tigres
from tigres.core.previous import PREVIOUS
from tigres.core.monitoring.log import log_template, log_node
from tigres.core.utils import TigresInternalException
from tigres.core.state.work import WorkSequence, WorkInputs, WorkParallel
from tigres.utils import State, TaskFailure, TigresException


class _LogWork(object):
    """
    Logging decorator that allows you to log  entry and exit points of run_work_*
    functions.  This decorator assumes that the first argument is a WorkBase object
    """
    # Customize these messages
    ENTRY_MESSAGE = 'Begin Work {}'
    EXIT_MESSAGE = 'End Work {}'

    def __init__(self):
        pass

    def __call__(self, func):
        """
        Returns a wrapper that wraps func.
        The wrapper will log the entry and exit points of the run_work function
        with Level.INFO level.
        """

        @functools.wraps(func)
        def wrapper(*args, **kwds):
            work = args[0]
            log_args = { Keyword.WORK_UID: work.name}
            if work.parent is not None:
                # FIXME - what is the best way to handle this name formatting?
                log_args[Keyword.TMPL_UID] = work.parent.name.replace(" ", "+")
            log_node(Level.INFO, work.name, state=work.state, nodetype=work.get_type_name(),
                     message=_LogWork.ENTRY_MESSAGE.format(work.name), **log_args)
            f_result = func(*args, **kwds)
            log_node(Level.INFO, work.name, state=work.state, nodetype=work.get_type_name(),
                     message=_LogWork.EXIT_MESSAGE.format(work.name), **log_args)
            return f_result
        return wrapper


def get_results():
    """
    Get the results from the latest task or template of the running Tigres program

    :return:
    """
    # Get the Tigres Program
    program = Program()

    return program.root_work.results


def prepare_work(work):
    """
    Prepare the given state for execution. Resolve the PREVIOUS syntax
    which results in a new set of input values being created.

    :param work: the state the prepared for execution
    """
    if work.state not in (State.NEW, State.RUN, State.READY):
        raise TigresInternalException(
            "Error trying to run state. Cannot run state! Invalid state {}".format(
                work.state))

    # Was this state already started?
    if work.state is State.NEW:
        _validate_inputs(work)

        # Evaluate the state inputs for PREVIOUS
        copy_input_values = execute_previous_syntax(work)
        work.state = State.READY
        if copy_input_values:
            work.inputs = WorkInputs(work.inputs.task, copy_input_values)


def run_work(work):
    """
    Executes a :class:`WorkUnit`

    :rtype : list, object or TaskFailure
    :return: task's output
    :raises: Exception if there are errors resolving inputs
    """

    prepare_work(work)

    try:

        work.state = State.RUN
        work.results, work.state = tigres.core.execution.engine.execute(
            work.name, work.inputs.task,
            work.inputs.values,
            work.execution_data)

        # If this is a sequence we are running sequence state wait until state is finished.
        while isinstance(work.parent, WorkSequence) and work.state in (
                State.READY, State.RUN):
            work.results, work.state = tigres.core.execution.engine.execute(
                work.name, work.inputs.task,
                work.inputs.values, work.execution_data)

    except Exception as err:
        work.results, work.state = TaskFailure(
            name="{} task failed".format(work.name), error=err), State.FAIL
        raise err

@_LogWork()
def run_work_sequence(sequence_work, task_array, input_array, env=None):
    """
    Executes a :class:`WorkSequence`

    :param sequence_work:  The sequence  state that the state is running in
    :type sequence_work: tigres.core.state.SequenceWork
    :param task_array: Th array of tasks to run
    :type task_array: TaskArray
    :param input_array: The array of inputs for the tasks
    :type input_array: InputArray
    :param env: keyword arguments for the template task execution environment
    :return: results of the last task
    """
    validate_tigres_object('task_array', TaskArray, task_array)
    validate_tigres_object('input_array', InputArray, input_array)

    if not env:
        env = {}

    work_generator = _WorkGenerator(task_array, input_array, fill=False,
                                    env=env)
    for work in work_generator:
        sequence_work.append(work)
        run_work(work)

        # Check for a Failure
        if isinstance(work.results, TaskFailure):
            raise work.results.error

@_LogWork()
def run_work_parallel(parallel_work, task_array, input_array, env=None):
    """
    Executes a :class:`WorkParallel`

    :param parallel_work: The parallel state that the state units are running in
    :type parallel_work: tigres.core.state.ParallelWork
    :type task_array: TaskArray
    :param input_array: The array of inputs for the tasks
    :type input_array: InputArray
    :param env: keyword arguments for the template task execution environment
    :return: results of all tasks
    :rtype: list
    """
    validate_tigres_object('task_array', TaskArray, task_array)
    validate_tigres_object('input_array', InputArray, input_array)

    if not env:
        env = {}

    work_generator = _WorkGenerator(task_array, input_array, env=env)
    parallel_fn = tigres.core.execution.engine.parallel
    for work in work_generator:
        _validate_inputs(work)
        parallel_work.append(work)
        prepare_work(work)

    parallel_fn(parallel_work, run_work)
    if parallel_work.state == State.FAIL:
        raise TigresException("One or more parallel tasks failed.")


def run_template_sequence(name, task_array, input_array, env=None):
    """
    Runs  a :class:`WorkSequence` which is an ordered list of :class:`WorkUnit`

    :param name:
    :param task_array:
    :param input_array:
    :param env: keyword arguments for the template task execution environment
    :return:
    """

    validate_tigres_object('task_array', TaskArray, task_array)
    validate_tigres_object('input_array', InputArray, input_array)

    from tigres.core.state.program import Program
    # Get the Tigres Program
    program = Program()

    # Register the sequence state with the Tigres Program
    sequence_work = program.register_sequence_work(name)
    program.root_work.append(sequence_work)

    # Run the sequence state
    log_template(sequence_work.name, State.RUN, 'sequence')
    run_work_sequence(sequence_work, task_array, input_array, env=env)
    log_template(sequence_work.name, sequence_work.state, 'sequence')

    # Done let's return results
    return sequence_work.results


def run_template_parallel(name, task_array, input_array, env=None):
    """
    Runs a :class:`WorkParallel` which is an ordered list of :class:`WorkUnit`
    :param name:
    :param task_array:
    :param input_array:
    :return:
    """
    validate_tigres_object('task_array', TaskArray, task_array)
    validate_tigres_object('input_array', InputArray, input_array)

    from tigres.core.state.program import Program
    # Get the Tigres Program
    program = Program()

    # Check that the parallel tasks have values
    if not input_array:
        input_array = []
    if len(input_array) == 0:
        # Evaluate implicit PREVIOUS for the parallel template input
        previous_results = get_results()
        if not previous_results:
            raise ValueError(
                "Parallel template '{0:s}' is missing PREVIOUS parallel task input array".format(
                    name))
        try:
            for _ in previous_results:
                input_array.append([PREVIOUS.i])
        except Exception as e:
            raise ValueError(
                'Parallel template \'{0:s}\' failed trying  to use PREVIOUS result:'
                '{}'.format(name, str(e)))

    # Register the parallel state with the Tigres program
    parallel_work = program.register_parallel_work(name)
    program.root_work.append(parallel_work)

    # Run the Parallel Work
    log_template(parallel_work.name, State.RUN, 'parallel')
    run_work_parallel(parallel_work, task_array, input_array, env=env)
    log_template(parallel_work.name, parallel_work.state, 'parallel')

    # Done let's return the results
    return parallel_work.results


def run_template_split(name, split_task, split_input_values, task_array,
                       input_array, env=None):
    """
    Runs split template which consistes of a SequenceWork with Work then a ParallelWork

    :param name:
    :param split_task:
    :param split_input_values:
    :param task_array:
    :param input_array:
    :return: the results
    """
    validate_tigres_object('task_array', TaskArray, task_array)
    validate_tigres_object('input_array', InputArray, input_array)
    validate_tigres_object('split_task', Task, split_task)
    validate_tigres_object('split_input_values', InputValues,
                           split_input_values)

    if not env:
        env = {}

    # Evaluate the implicit PREVIOUS syntax
    split_input_values = evaluate_implicit_previous(split_task,
                                                    split_input_values)

    # Get the Tigres Program
    from tigres.core.state.program import Program

    program = Program()

    # Register the split sequence template with the Tigres Program
    split_sequence_work = program.register_sequence_work(name)
    program.root_work.append(split_sequence_work)

    # Register the split state with the Tigres Program
    log_template(split_sequence_work.name, State.RUN, 'split')
    split_work = program.register_work(
        WorkInputs(split_task, split_input_values))
    split_work.execution_data.setdefault('env', {})
    split_work.execution_data['env'].update(env)
    split_sequence_work.append(split_work)

    # run the split state
    run_work(split_work)

    # Run the split parallel state
    # if the input list is empty this means process the previous results
    if len(input_array) == 0:
        # Evaluate implicit PREVIOUS for the merge template input
        previous_results = get_results()
        if not previous_results:
            raise ValueError(
                "Split template '{0:s}' is missing parallel task input array".format(
                    name))
        try:
            for _ in previous_results:
                input_array.append([PREVIOUS.i])
        except Exception as e:
            raise ValueError(
                'Split template \'{0:s}\' failed trying  to use PREVIOUS split task result:'
                '{}'.format(name, str(e)))

    # Register the split parallel state with the Tigres Program
    split_parallel_work = program.register_parallel_work(name)
    split_sequence_work.append(split_parallel_work)

    input_array = _validate_split_parallel_task(name, task_array, input_array)
    run_work_parallel(split_parallel_work, task_array, input_array, env=env)
    log_template(split_sequence_work.name, split_sequence_work.state, 'split')

    # Done, let's return the results
    return split_sequence_work.results


def run_template_merge(name, task_array, input_array, merge_task,
                       merge_input_values, env=None):
    """
    Runs a merge template composed of a SequenceWork with a ParallelWork then a Work

    :param name:
    :param task_array:
    :param input_array:
    :param merge_task:
    :param merge_input_values:
    :return:
    """
    validate_tigres_object('task_array', TaskArray, task_array)
    validate_tigres_object('input_array', InputArray, input_array)
    validate_tigres_object('merge_task', Task, merge_task)
    validate_tigres_object('merge_input_values', InputValues,
                           merge_input_values)

    if not env:
        env = {}

    # Verify that there is a parallel task array
    if not task_array or len(task_array) == 0:
        raise ValueError(
            "Merge template '{0:s}' is missing the parallel tasks. task_array is either empty or missing.".format(
                name))

    # Get the Tigres Program
    from tigres.core.state.program import Program

    program = Program()

    # Check that the parallel tasks have values
    if not input_array:
        input_array = []
    if len(input_array) == 0:
        # Evaluate implicit PREVIOUS for the merge template input
        previous_results = get_results()
        if not previous_results:
            raise ValueError(
                "Merge template '{0:s}' is missing PREVIOUS parallel task input array".format(
                    name))
        try:
            for _ in previous_results:
                input_array.append([PREVIOUS.i])
        except Exception as e:
            raise ValueError(
                'Merge template \'{0:s}\' failed trying  to use PREVIOUS result:'
                '{}'.format(name, str(e)))

    # Register a sequence for the merge template with the Tigres Program
    merge_sequence_work = program.register_sequence_work(name)
    program.root_work.append(merge_sequence_work)

    # Register the merge parallel state with the Tigres Program
    log_template(merge_sequence_work.name, State.RUN, 'merge')
    merge_parallel_work = program.register_parallel_work(name)
    merge_sequence_work.append(merge_parallel_work)

    # run parallel state
    run_work_parallel(merge_parallel_work, task_array, input_array, env=env)

    # Register the merge state with the Tigres Program
    # Evaluate the implicit PREVIOUS syntax
    merge_input_values = evaluate_implicit_previous(merge_task,
                                                    merge_input_values)

    merge_work = program.register_work(
        WorkInputs(merge_task, merge_input_values))
    merge_work.execution_data.setdefault('env', {})
    merge_work.execution_data['env'].update(env)
    merge_sequence_work.append(merge_work)

    # Run Merge Work
    run_work(merge_work)
    log_template(merge_sequence_work.name, merge_sequence_work.state, 'merge')

    # Done, let's return the results
    return merge_sequence_work.results


def evaluate_implicit_previous(task, input_values, previous_syntax=PREVIOUS):
    """
    Inspects the given task array for implicit PREVIOUS condition.
    If condition found, then PREVIOUS explicitly specified in the given inputs

    Implicit PREVIOUS is not evaluated if task is missing or task input types are of
    zero length. Implicit PREVIOUS will be evaluated if the InputValues are missing.
    InputTypes are ignored.

    :param task: the task to evaluate
    :param input_values:
    :type input_values: list or None
    :param previous_syntax: the previous syntax to implicitly use (Default PREVIOUS)
    :return: a new input values object
    :rtype: InputValues


    """

    # check to see if there is a task with input types
    return_input_values = input_values
    if task:
        if input_values is None:

            # instantiate a new list
            new_input_values = []

            new_input_values.append(previous_syntax)
            return_input_values = new_input_values
        else:
            # copy the original input values
            return_input_values = copy(input_values)

    return return_input_values


def execute_previous_syntax(work):
    """
    Evaluates any input values that use the PREVIOUS syntax.
    Returns a new input array with the resolved PREVIOUS values.

    :param work:
    :type work: WorkUnit

    :return: input_values
    :rtype: list
    """

    # Shallow copy of input values
    copy_input_values = copy(work.inputs.values)
    for i in range(len(copy_input_values)):
        # resolve the PREVIOUS values
        if copy_input_values[i] is PREVIOUS:
            # Instantiate PREVIOUS for usage
            copy_input_values[i] = copy_input_values[i]()
        if isinstance(copy_input_values[i], PREVIOUS):
            # Execute PREVIOUS
            copy_input_values[i] = copy_input_values[i](work.index)
    return copy_input_values


class _WorkGenerator:
    """
    Generates a list of newly registered Work objects for the specified task array and input array.

    """


    def __init__(self, task_array, input_array, fill=True, env=None):
        """
        Generates a list of newly registered Work objects for the specified task array and input array.
        During initialization a comparison of the task and input arrays occurs. If ``fill`` is ``True``,
        then tasks or inputs are implied if the arrays are of unequal length.

        :param fill: Should the inputs or tasks be copied if task and input arrays are unequal?
        :type fill: bool
        :param task_array: The tasks used to generate each piece of Work
        :type task_array: list
        :param input_array: The inputs used to generate each piece of Work
        :type input_array: list
        :param env: keyword arguments for the task execution environment
        :return: a state generator
        :rtype: _WorkGenerator
        :raises WorkGenerationException
        """
        self._task_array = task_array
        self._input_array = input_array
        self._values = []
        self._index = 0

        if not self._task_array or len(self._task_array) == 0:
            raise ValueError("The tasks are missing")
        if self._input_array is None:
            raise ValueError("The inputs are missing")

        len_task_array = len(task_array)
        len_input_array = len(input_array)

        # This will default to  PREVIOUS
        fillvalue = [PREVIOUS]
        if len_input_array != len_task_array:
            if fill:
                if len_task_array > len_input_array:
                    if len_input_array == 1:
                        fillvalue = input_array[-1]
                    elif len_input_array > 1:
                        raise ValueError(
                            "Task and input lists are unequal length. There is no implicit interpretation" +
                            " of inputs for {0:d} tasks and {1:d} inputs. ".format(
                                len_task_array, len_input_array))
                elif len_task_array < len_input_array:
                    if len_task_array == 1:
                        fillvalue = task_array[-1]
                    elif len_task_array > 1:
                        raise ValueError(
                            "Task and input lists are unequal length. There is no implicit interpretation" +
                            " of inputs for {0:d} tasks and {1:d} inputs. ".format(
                                len_task_array, len_input_array))


        # Generate the WorkInputs to register as state with the Tigres Program
        for idx, item in enumerate(
                zip_longest(task_array, input_array, fillvalue=fillvalue)):
            inputs = WorkInputs(item[0], item[1])
            work = Program().register_work(inputs)
            work.execution_data.setdefault('env', {})
            work.execution_data['env'].update(env)
            work.index = idx
            self._values.append(work)

    def __iter__(self):
        return self

    def next(self):
        # Python 3 compatible
        return self.__next__()

    def __next__(self):
        """

            :return: the next piece of state
            :rtype: WorkUnit
            """
        if self._index < len(self._values):
            value = self._values[self._index]
        else:
            raise StopIteration
        self._index += 1
        return value


def _validate_inputs(work):
    """
    Validate the task's input values for missing values and
    valid uses of the PREVIOUS syntax

    :param work: The state unit to validate
    :type work: tigres.core.state.WorkUnit
    :return: None
    :raises: ValueError
    """

    # Are there input values?
    if work.inputs.values:
        # There are input values but are there any values?
        if not isinstance(work.inputs.values, (list, tuple)):
            raise ValueError(
                "Invalid input values for Task '{}'. Input values is a '{}' it must be a 'list' or 'tuple'".format(
                    work.inputs.task.name, type(work.inputs.values).__name__))
            # Examine the values for valid use of the PREVIOUS syntax
        for v in work.inputs.values:
            if v is PREVIOUS or isinstance(v, PREVIOUS):
                if Program().previous_work:
                    # No value error was thrown by Program().previous
                    # We can continue
                    pass
    elif not work.inputs.values and work.inputs.task.input_types and len(
            work.inputs.task.input_types) > 0:
        # Input Values are missing, raise an error
        raise ValueError(
            "Task {0:s} with Work Id {1:s} is missing it's input values".format(
                work.inputs.task.name, work.name))


def _validate_split_parallel_task(name, task_array, input_array):
    """
    Validate the split parallel task. Split Parallel Tasks do not
    imply PREVIOUS syntax when values are missing.

    :param name: The name of the split task
    :param task_array: The split parallel task array
    :param input_array: The split parallel input array
    :return: input array for split parallel task
    :raises: ValueError
    """
    # Verify that there is a parallel task array
    if not task_array or len(task_array) == 0:
        raise ValueError(
            "Split template '{0:s}' is missing the parallel tasks. task_array is either empty or missing.".format(
                name))

    # Check that the parallel tasks have values
    if not input_array or len(input_array) == 0:
        # No Values exist, Now we need to determine if there is an error here
        for task in task_array:
            # Use the task input_types to determine if the Split Parallel
            # Task expects inputs
            if task.input_types:
                if len(task.input_types) > 0:
                    # ERROR the Split Parallel Task Expects inputs,
                    raise ValueError(
                        "Split template '{0:s}' parallel task '{1:s}' expects {2:d} inputs."
                        .format(name, task.name, len(task.input_types)))
        input_array = []

    return input_array


