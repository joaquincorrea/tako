"""
`tigres.core.state.program`
================================

.. currentmodule:: tigres.core.state.program

:platform: Unix, Mac
:synopsis: Tigres program state


Classes
-------

 * :class:`Program` - A global Tigres program. Used to maintain program state.
 * :class:`ProgramExecutionError` - Exception class for :class:`Program` errors

.. moduleauthor:: Val Hendrix <vhendrix@lbl.gov>



"""
import time
from uuid import uuid4
import threading

from tigres.core.execution import load_plugin
from tigres.core.utils import SingletonMeta
from tigres.core.state.work import WorkUnit, WorkParallel, WorkSequence, \
    Identifier, CURRENT_INDEX, PREVIOUS_INDEX
from tigres.utils import get_new_output_file, State, Execution
from tigres.core.monitoring.log import init, finalize, log_node
from tigres.core.monitoring.common import Keyword, Level, NodeType


class ProgramExecutionError(Exception):
    pass

# Python 2 and 3 metaclass compatibility
_ProgramSingletonMeta = SingletonMeta('ProgramSingletonMeta', (object, ), {})


class Program(_ProgramSingletonMeta):
    """
        Global tigres program. This is a singleton class used
        for managing the state of the tigres program.

        :Example:

        >>> from tigres.core.state.program import Program
        >>> program1 = Program()
        >>> program2 = Program()
        >>> id(program1) == id(program2)
        True

    """

    def __init__(self, name=None, log_dest=None,
                 execution=Execution.LOCAL_THREAD, env=None, **kwargs):
        """
        Initializes a new Tigres Program

        :param self:
        :param execution: The execution plugin to use. Default: :class:`Execution.LOCAL_THREAD`
        :param env: environment variable for task execution
        :type env: dict
        :param kwargs:
        :param name: Workflow name. If not given, a random one will be chosen.
        :type name: str
        :param log_dest: Log destination, passed as `dest` to `monitoring.api.init()`
        :param log_*: Additional keywords to pass to :func:`monitoring.log.init()`, with the `log_` prefix stripped
        :return: program
        :rtype: Program

        """

        self._lock = threading.Lock()
        if name is None:
            # Autogenerate a name
            name = "Tigres{:x}".format(int(time.time()))

        # All Tasks are registered here
        self._tigres_objects = {}
        self._env = env

        # All state is registered here
        self._work = {}

        self._name = name
        self._identifier = str(uuid4())

        # init monitoring
        monitoring_kw = {k[4:]: v for k, v in kwargs.items() if
                         k.startswith("log_") and k != "log_dest"}

        self._log_dest = log_dest
        if log_dest is None:
            self._log_dest = get_new_output_file(
                basename='tigres-{}'.format(name))
        init(self._log_dest, self.name, self._identifier, **monitoring_kw)
        self._log(Level.INFO, "start", state=State.RUN)
        self._log(Level.INFO, Keyword.pfx + "init_program",
                  message="initializing program")

        # This must happen after the logging is initialized
        # We might want to revisit whether we need to log
        # every time a name is registered with the program
        self._root_sequence_work = self.register_sequence_work(name)

        load_plugin(execution)
        self._log(Level.INFO, Keyword.pfx + "load_execution", message=execution)

    @staticmethod
    def _get_keys_by_name(name, dictionary):
        like_names = [key for key in list(dictionary.keys()) if
                      key.name == name]
        return like_names

    def _get_unique_identifier(self, name, dictionary):
        # Are there any names already registered?
        like_names = self._get_keys_by_name(name, dictionary)
        len_names = len(like_names)
        # This name exists, create a new WorkId
        identifier = Identifier(name, len_names)
        return identifier

    def _log(self, level, event, nodetype=NodeType.PROGRAM, **kwargs):
        """
        Log messages for the program
        :param level:
        :param status:
        :param kwargs:
        :return:
        """
        log_node(level, self._name, event=event, nodetype=nodetype,
                 **kwargs)

    def _register_work(self, name, work_class):
        """
        base register state method

        :param name: name of state
        :param work_class: the state class to create
        :type work_class: Work, ParallelWork, SequenceWork
        :return: new state object
        """
        if not name and work_class:
            raise ProgramExecutionError(
                "Work Registration error: A name must be specified for class %s" % work_class)
        elif not name:
            raise ProgramExecutionError(
                "Work Registration error: A name and state class must be specified to register state")
        if not work_class:
            raise ProgramExecutionError(
                "Work Registration error: A Work class must be specified for %s" % name)
        parent = None

        with self._lock:
            if hasattr(self, '_root_sequence_work') and len(self._root_sequence_work) > 0:
                if work_class is WorkUnit:
                    parent = self._root_sequence_work[CURRENT_INDEX]
            elif hasattr(self, '_root_sequence_work'):
                parent = self._root_sequence_work

            work_id = self._get_unique_identifier(name, self._work)
            work = work_class(parent, work_id)
            self._work[work_id] = work
            self._log(Level.INFO, "register",
                      message="registering {} '{}' ".format(type(work).__name__, work.name))
            return work

    @staticmethod
    def clear():
        """
        Clear the program
        :return: None
        """
        if hasattr(Program, '_instance') and Program._instance:
            Program()._log(Level.INFO, "end", state=State.DONE)
            finalize()
            Program._instance = None

    @property
    def env(self):
        return self._env

    @property
    def identifier(self):
        return self._identifier

    @property
    def name(self):
        with self._lock:
            if hasattr(self, '_root_sequence_work'):
                return self._root_sequence_work.name
            else:
                return self._name

    @property
    def previous_work(self):
        """
        Return the :class:`WorkBase` from the previous execution.
        """
        with self._lock:
            current_work = self._root_sequence_work[CURRENT_INDEX]
            is_sequence = isinstance(current_work, WorkSequence)
            if len(current_work) > 1 and is_sequence:
                # The current state is a sequence, and there is previous task
                # available
                return self._root_sequence_work[CURRENT_INDEX][PREVIOUS_INDEX]
            elif len(self._root_sequence_work) > 1:
                # The current state is either parallel or it is the first
                # task in a sequence.  Therefore, the results of the
                # previous template is returned
                return self._root_sequence_work[PREVIOUS_INDEX]
            else:
                # There are not previous tasks to retrieve.
                raise ValueError("There is no previous state available.")

    def get_work_by_name(self, name):
        """
        Get all the state objects that have the specified name in their
        WorkId.

        :param name:
        :return: Work objects with the name
        :rtype: list
        """
        with self._lock:
            return self._get_keys_by_name(name, self._work)

    @property
    def root_work(self):
        """
        Root state object
        :return: the root state object
        :rtype: tigres.core.state.WorkSequence
        """
        with self._lock:
            return self._root_sequence_work

    def register(self, item):
        """

        :param item: The item to register
        :return: the new identifier
        """
        with self._lock:
            identifier = None
            if hasattr(item, "_identifier"):
                identifier = self._get_unique_identifier(item.name,
                                                         self._tigres_objects)
                self._tigres_objects[identifier] = item
                name = identifier.name
                if identifier.index > 0:
                    name += "-{}".format(identifier.index)
                self._log(Level.INFO, "register",
                          message="registering identifier '{}' for {} with name '{}' ".format(
                              name, type(item).__name__,
                              item.name))
            return identifier

    def register_work(self, work_inputs):
        """
        Register the given WorkInputs as new state.
        :param work_inputs: a new Work object with a unique WorkId
        :return: a new state object
        :rtype: tigres.core.state.WorkUnit

        """
        if not work_inputs:
            raise ProgramExecutionError(
                "Work inputs for %s are missing" % work_inputs.task.name)
        work = self._register_work(work_inputs.task.unique_name, WorkUnit)
        work.inputs = work_inputs
        work.execution_data.setdefault('env', {})
        if self._env:
            work.execution_data['env'].update(self._env)
        return work

    def register_parallel_work(self, name):
        """
        Register the given name as new ParallelWork.
        :param name: user specified name
        :return: new state object
        :rtype: tigres.core.state.WorkParallel
        """
        return self._register_work(name, WorkParallel)

    def register_sequence_work(self, name):
        """
        Register the given name as new SequenceWork
        :param name: user specified name
        :return: new state object
        :rtype: tigres.core.state.WorkSequence
        """
        return self._register_work(name, WorkSequence)
