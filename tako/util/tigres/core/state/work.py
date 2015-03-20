"""
`tigres.core.state.work`
================================

.. currentmodule:: tigres.core.state.work

:platform: Unix, Mac
:synopsis: Tigres Work Model


Classes
-------

 * :class:`WorkBase` - Base class for :class:`Program` work.
 * :class:`WorkParallel` - A single unit of Parallel work representing one or more :class:`WorkUnit` that are run in parallel.
 * :class:`WorkSequence` -  A single unit of Sequence work representing one or more :class:`WorkUnit` that are run in sequence.
 * :class:`WorkUnit` - A single unit of work that is executed.

Named Tuples
------------

 * `Identifier` - a :class:`namedtuple` ,``(name,index)``, for uniquely identifying Tigres types and Work.
 * `WorkInputs` - a :class:`namedtuple` ,``(task,values)``. A :class:`WorkUnit` has one of these to supply as input to the :mod:`tigres.core.execution` layer

Constants
---------

 * `CURRENT_INDEX`
 * `PREVIOUS_INDEX`

.. moduleauthor:: Val Hendrix <vhendrix@lbl.gov>


"""
from collections import namedtuple
import functools

from tigres.core.monitoring.common import Keyword, Level
from tigres.core.monitoring.log import log_task, log_node
from tigres.core.utils import TigresInternalException
from tigres.utils import State, TaskFailure

__author__ = 'val'

PREVIOUS_INDEX = -2
CURRENT_INDEX = -1

# ## Named Tuple representing the inputs for an execution of Work
WorkInputs = namedtuple("WorkInputs", "task values")

### Unique  identifier
Identifier = namedtuple("Identifier", "name index")


def log_change_handler_state(inst, log_args, value):
    """
    Log change hander function for WorkUnit.state.

    :param inst:
    :param log_args:
    :param value:
    :return:
    """
    assert isinstance(inst, WorkUnit)
    assert isinstance(log_args,dict)

    log_args[Keyword.STATE] = value
    if value == State.FAIL:
        log_args[Keyword.STATUS] = -1
        if isinstance(inst.results, TaskFailure):
            log_args[Keyword.ERROR] = str(inst.results.error).replace(
                "\n", "; ")
        return Level.ERROR
    return Level.INFO


class _LogChangeProperty(object):
    """
    Computes attribute value and caches it in the instance.
    Source: Python Cookbook
    Author: Denis Otkidach http://stackoverflow.com/users/168352/denis-otkidach
    This decorator allows you to create a property which can be computed once and
    accessed many times. Sort of like memoization
    """
    def __init__(self, name, default=None, handler=None):
        self.name = name
        self.default = default
        self.handler = handler

    def __get__(self, inst, owner):

        inst.__dict__.setdefault(self.name, self.default)
        return inst.__dict__[self.name]

    def __set__(self, inst, value):
        inst.__dict__.setdefault(self.name, self.default)
        if value != inst.__dict__[self.name]:
            inst.__dict__[self.name] = value
            log_args = {Keyword.TASK_UID: inst.name}
            log_level = Level.INFO
            # TODO - research better way to figure out who the parent is
            # At this point we determine the name of the template which is
            # one level below the root_work.  This will be revisited with
            # nested templates
            if inst.parent:
                if inst.parent.parent and inst.parent.parent.parent and inst.parent.parent.parent.parent:
                    log_args[Keyword.TMPL_UID] = inst.parent.parent.parent.name.replace(" ", "+")
                elif inst.parent.parent and inst.parent.parent.parent:
                    log_args[Keyword.TMPL_UID] = inst.parent.parent.name.replace(" ", "+")
                else:
                    log_args[Keyword.TMPL_UID] = inst.parent.name.replace(" ", "+")
            if self.handler:
                log_level = self.handler(inst, log_args, value)
            else:
                log_args[Keyword.EVENT] = self.name
            log_node(log_level, inst.name, nodetype=inst.get_type_name(), **log_args)



class WorkBase(object):
    """
    Base object for all state.
    """

    def get_type_name(self):
        return NotImplemented

    def __init__(self, work_id, parent_work=None):
        """

        The Base Work object

        :type self: WorkBase
        :param work_id: unique state id.
        :type work_id: WorkName

        """
        if isinstance(work_id,str):
            # FIXME - need to prevent '#' from being
            # used in names.
            split_name = work_id.split("#")
            idx =0
            if len(split_name) > 1:
                idx = split_name[1]
            self._id = Identifier(split_name[0], idx)
        else:
            self._id = work_id
        self._parent = parent_work

    @property
    def name(self):
        """
        The unique name of the state
        :return: name
        :rtype: str
        """
        if self._id.index == 0:
            return self._id.name
        else:
            # FIXME - '#' need to be disallowed in names
            return "{}#{}".format(self._id.name, self._id.index)

    @property
    def parent(self):
        return self._parent

    @property
    def previous(self):
        """
        Return
        :return: state previous to this one
        """
        raise NotImplementedError

    @property
    def results(self):
        raise NotImplementedError


class WorkUnit(WorkBase):
    """
     A single unit of state that is executed.
    """

    def get_type_name(self):
            return 'task'

    def __init__(self, parent_work, work_id, state=State.UNKNOWN, inputs=None):
        """
        A single unit of state that is executed

        :param work_id: unique state id.
        :type work_id: WorkName
        :param state: state state (UNKNOWN, RUN, DONE, FAIL). Default is UNKNOWN
        :param inputs: The inputs for an execution of the state
        :type inputs: WorkInputs
        :return: Work

        :Example:

        >>> from tigres.core.state.work import WorkUnit
        >>> WorkUnit(work_id="MyAwesomeTask",parent_work=None)
        Work(id='MyAwesomeTask')

        """
        WorkBase.__init__(self, work_id, parent_work=parent_work)
        self._state = state
        self._results = None
        self._inputs = inputs
        self._index = 0
        self._execution_data = {}

    def __str__(self):
        return "Work(id='%s')" % self._id

    __repr__ = __str__


    @property
    def previous(self):
        """
        Return the state previous to this.

        :return:
        """
        if self.parent:
            for index, work in enumerate(self.parent):
                if work == self and index > 0:
                    return self.parent[index - 1]

            return self.parent.previous
        return None

    @property
    def execution_data(self):
        return self._execution_data

    @property
    def index(self):
        return self._index

    @index.setter
    def index(self, value):
        self._index = value

    @property
    def inputs(self):
        """
        The inputs (:class:`WorkInputs`) for an execution of the state
        """
        return self._inputs

    @inputs.setter
    def inputs(self, value):
        self._inputs = value

    @property
    def results(self):
        """
        The results of the state execution. This could
        be a failure or actual results
        :return: results
        :rtype: tigres.utils.TaskFailure or list
        """
        return self._results

    @results.setter
    def results(self, value):
        self._results = value

    state = _LogChangeProperty('_state', State.UNKNOWN, log_change_handler_state)



class WorkParallel(list, WorkBase):
    """
    A single unit of Parallel state representing one or more :class:`WorkUnit` that are run in parallel.
    """

    def get_type_name(self):
        return 'parallel'

    def __init__(self, parent_work, work_id):
        """

         A single unit of Parallel state representing one or more
         :class:`WorkUnit` that are run in parallel.

        :param work_id: unique state id.
        :type work_id: WorkName

        """
        #noinspection PyTypeChecker
        list.__init__(self)
        WorkBase.__init__(self, work_id, parent_work=parent_work)

    @property
    def previous(self):
        """

        :return: The previous that was run
        :rtype: Work ParallelWork or WorkSequence or WorkUnit
        """
        if self.parent:
            for index, work in enumerate(self.parent):
                if work == self and index > 0:
                    return self.parent[index - 1]

            return self.parent.previous
        return None

    @property
    def results(self):
        """

        :return: results all of the state execution. This could
            be a failures and or actual results
        :rtype: list
        """
        # return ordered results
        results = [w.results for w in self]
        return results

    @property
    def state(self):
        """
        Returns the state of the parallel work.


        * NEW - All tasks are new
        * READY - All tasks are either READY or NEW
        * RUN - Any task is in the RUN State
        * DONE - All tasks are in the DONE state
        * FAIL - Any task is in the FAIL State but no tasks are currently running
        * UNKNOWN - State is not understood.

        """
        has_ready = False
        has_new = False
        has_done = False
        has_fail = False
        for w in self:
            if w.state == State.RUN:
                return State.RUN
            if w.state == State.FAIL:
                has_fail = True
            if w.state == State.READY:
                has_ready = True
            if w.state == State.NEW:
                has_new = True
            if w.state == State.DONE:
                has_done = True

        if not has_ready and not has_new and has_fail:
            return State.FAIL
        elif (has_ready or has_new) and (has_done or has_fail):
            return State.RUN
        elif has_ready or has_new:
            if not has_ready:
                return State.NEW
            return State.READY
        elif not has_ready and not has_new and has_done:
            return State.DONE
        else:
            return State.UNKNOWN

    def append(self, p_object):
        if not isinstance(p_object, WorkBase):
            raise TigresInternalException(
                "Must add WorkBase to {} not {}".format(self.__class__.__name__,
                                                        type(p_object)))
            # Add self as parent of this state
        p_object._parent = self
        list.append(self, p_object)

        if isinstance(p_object, WorkUnit) and p_object.state is State.UNKNOWN:
            # Only set to NEW if UNKNOWN (this is a new object)
            # Might want to reconsider this logic
            p_object.state = State.NEW


class WorkSequence(list, WorkBase):
    """
    A single unit of Sequence state representing one or
        more :class:`WorkUnit` that are run in sequence.

    """

    @classmethod
    def get_type_name(self):
        return 'sequence'

    def __init__(self, parent_work, work_id):
        """

        A single unit of Sequence state representing one or
        more :class:`WorkUnit` that are run in sequence.

        :param work_id: unique state id.
        :type work_id: WorkName

        """
        #noinspection PyTypeChecker,PyTypeChecker
        list.__init__(self)
        WorkBase.__init__(self, work_id, parent_work=parent_work)

    def append(self, p_object):
        if not isinstance(p_object, WorkBase):
            raise TigresInternalException(
                "Must add WorkBase to {} not {}".format(self.__class__.__name__,
                                                        type(p_object)))
        # Add self as parent of this state
        p_object._parent = self
        list.append(self, p_object)
        if isinstance(p_object, WorkUnit)  and p_object.state is State.UNKNOWN:
            # Only set to NEW if UNKNOWN (this is a new object)
            # Might want to reconsider this logic
            p_object.state = State.NEW


    @property
    def previous(self):
        """
        The state previous to this

        :return: The previous that was run
        :rtype: Work ParallelWork or WorkSequence or WorkUnit
        """
        if self.parent:
            for index, work in enumerate(self.parent):
                if work == self and index > 0:
                    return self.parent[index - 1]

            return self.parent.previous
        return None


    @property
    def state(self):
        if self:
            return self[CURRENT_INDEX].state
        else:
            return State.NEW

    @property
    def results(self):
        if self:
            return self[CURRENT_INDEX].results
        else:
            return None
