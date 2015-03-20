"""
`tigres.core.task`
**********************

.. currentmodule:: tigres.core.task

:platform: Unix, Mac
:synopsis: The internal task api




The Tigres Task API contains classes that are exposed to the
user-level Tigres API.

Classes
=======


 * :class:`EXECUTABLE` - identifies that a task implementation is an executable
 * :class:`FUNCTION` - identifies that a task implementation is a function
 * :class:`InputArray` - Array of one or more InputValues, which will be inputs to a TaskArray in a Template.
 * :class:`InputTypes` - List of types for inputs of a Task.
 * :class:`InputValues` - List of values for inputs matched to a task Task.
 * :class:`Task` - Function or program. A task is the atomic unit of execution in Tigres.
 * :class:`TaskArray` - List of one or more Tasks, which will be executed in a Template
 * :class:`TaskFailure` - Object to be returned in case of failures in task execution
 * :class:`TigresException` - Exception thrown in the event of a runtime error
 * :class:`TigresObject` - Allow Tigres to track user-defined objects.
 * :class:`TigresType` -  Base class for all Tigres types


Functions
=========
 * :func:`get_number_args` - Get the number of arguments for the given callable implementation
 * :func:`validate_callable` - Validates that the specified input is callable
 * :func:`is_list` -  Determines of the specified object is a `list` using duck typing.
 * :func:`is_task` -  Determines of the specified object is a `Task` using duck typing.
 * :func:`is_tigres_object` -  Determine if the specified tigres object is a tigres class using duck typing
 * :func:`validate_tigres_object` - Validate that the given tigres object represents this given tigres_class.

.. moduleauthor:: Val Hendrix <vhendrix@lbl.gov>



"""

from abc import ABCMeta
from inspect import isclass
from inspect import getargspec, isfunction
import inspect
from uuid import uuid4
import collections

from tigres.core.monitoring import Program
from tigres.core.utils import get_metaclass
from tigres.utils import TigresException


EXECUTABLE = "EXECUTABLE"
FUNCTION = "FUNCTION"

LIST_ATTRIBUTES = [p for p in dir(list) if not p.startswith('_')]


class TigresObject(object):
    """Allows Tigres to track user-defined objects.

    Acts like the target object, except with added attributes:
        - tg_name: Name
        - tg_oid: Object ID
        - tg_attr: Dict of attributes
    """

    PREFIX = 'tg_'
    PREFIX_LEN = len(PREFIX)

    def __init__(self, obj, name=None, **kwargs):
        """Create wrapped object.

        :param obj: Target object to wrap
        :param name: Name of object (in logs, etc.)
        :type name: str
        """
        self._target = obj
        self._tg_attr = {'name': name, 'oid': str(uuid4()),
                         'type': obj.__class__.__name__}
        self._tg_attr.update(kwargs)

    @property
    def tg_attr(self):
        """Return all Tigres special attributes as a dictionary.
        """
        return self._tg_attr

    def __getattr__(self, a_name):
        """Get attribute on target class unless the attribute name
        starts with self.PREFIX; in this case, get the wrapper's value.
        """
        if a_name.startswith(self.PREFIX):
            try:
                r = self._tg_attr[a_name[self.PREFIX_LEN:]]
            except KeyError:
                raise AttributeError(a_name)
        else:
            r = getattr(self._target, a_name)
        return r


class TigresType(get_metaclass(ABCMeta, 'TigresTypeABCMeta')):
    """ Base class for all Tigres types

    """

    def __init__(self, name=None):

        self._name = name
        if not self._name:
            self._name = "%s_%s" % (self.__class__.__name__, id(self))

        # Make sure the name is a string
        if not isinstance(self._name, str):
            raise TigresException(
                'name for {} must be a string'.format(self.__class__.__name__))

        if not isinstance(self._name, str):
            raise TigresException(
                "The tigres object name must be a string. Got %s of %s" % (
                    str(self._name), type(self._name)))

        self._identifier = None
        self._identifier = Program().register(self)

    @property
    def name(self):
        return self._name

    @property
    def unique_name(self):
        if self._identifier.index == 0:
            return self._name
        return "%s-%s" % (self._identifier.name, self._identifier.index)

    @classmethod
    def __subclasshook__(cls, c):
        if cls is TigresType:
            if any(s in B.__dict__ for s in ["tigresName", "name"] for B in
                   c.__mro__):
                return True
        return NotImplemented

    def __subclass_properties(self):
        return dict(
            (a, b) for a, b in self.__dict__.items() if a.startswith('_') \
            and not a in ['_tigres_name', '_name'])

    def __repr__(self):
        repr_str = "{'name':'%s', " % self.name
        repr_str += "'unique_name':'%s' " % self.unique_name
        for (k, v) in self.__subclass_properties().items():
            if not k == "_identifier":
                repr_str += ", '%s':" % k[1:]
                if isinstance(v, str):
                    repr_str += "'%s'" % str(v)
                elif isclass(v):
                    repr_str += "%s" % v.__name__
                else:
                    repr_str += "%s" % repr(v)
        repr_str += "}"
        return repr_str

    def __str__(self):
        a_str = "<%s.%s name:%s\n" % (
            __name__, self.__class__.__name__, self._name )
        for (k, v) in self.__subclass_properties().items():
            if isclass(v):
                value_str = v.__name__
            else:
                value_str = repr(v)
            a_str += "    %s:%s\n" % (k[1:], value_str)
        if isinstance(self, list):
            a_str += " %s" % list(self)
        a_str += ">"
        return a_str


class TigresListType(TigresType, list):
    """
    Base class for all tigres list types
    """

    def __init__(self, name=None, list_=None):
        """Construct a new instance.

        :param name: User-defined name, e.g. "my tasks"
        :type name: str or None
        :param list_: List of itmes in the array. This list is mutable.
        :type list_: list
        """
        if list_:
            # noinspection PyTypeChecker
            list.__init__(self, list_)
        else:
            # noinspection PyTypeChecker
            list.__init__(self)
        TigresType.__init__(self, name=name)


class Task(TigresType):
    """Function or program. \
    A task is the atomic unit of execution in Tigres.

    :Example:

    >>> def abide(dude): return dude - 1
    >>> from tigres import Task, FUNCTION, EXECUTABLE
    >>> fn_task = Task("abiding", FUNCTION, abide)
    >>> exe_task = Task("more_abiding", EXECUTABLE, "abide.sh")
    """


    def __init__(self, name, task_type, impl_name, input_types=None, env=None):
        """Constructor.

        :param name: Name of the task. If None is given, one will be chosen.
        :type name: str or None
        :param task_type: The type of task being run.
        :type task_type: FUNCTION or EXECUTABLE
        :param impl_name: Function or path to executable.
        :type impl_name: function or str
        :param input_types: The input types associated with this task
        :type input_types: InputTypes or None or list
        :param env: The environment for this task
        :type env: dict or None
        """
        super(Task, self).__init__(name=name)

        self._task_type = task_type
        self._impl_name = impl_name
        self._input_types = input_types
        self._env = env
        if task_type is FUNCTION:
            validate_callable(impl_name)
            module = inspect.getmodule(self._impl_name)
            if self._impl_name and module and module.__name__.startswith("tigres."):
                raise ValueError("Task '{}' implementation of {} is not allowed to to be a Tigres callable".format(
                    self._name, self._impl_name.__name__))
            elif not self._impl_name:
                raise ValueError("Task '{}' implementation is missing".format(self._name))
        if input_types and not is_tigres_object(InputTypes, input_types):
            raise TypeError(
                "Invalid value for task. {} must be a tigres.InputTypes or list type".format(
                    type(input_types)))


    @property
    def task_type(self):
        return self._task_type

    @property
    def impl_name(self):
        return self._impl_name

    @property
    def input_types(self):
        return self._input_types

    @property
    def env(self):
        return self._env


class TaskArray(TigresListType):
    """List of one or more Tasks, which will be executed in a Template

    Instances act just like a list, except that they have a name.
    """

    def __init__(self, name=None, tasks=None):
        """Construct a new instance.

        :param name: User-defined name, e.g. "my tasks"
        :type name: str or None
        :param tasks: List of tasks in the array. This list is mutable.
        :type tasks: list
        """
        super(self.__class__, self).__init__(name, tasks)
        for t in self:
            if not is_task(t):
                raise TypeError(
                    "Invalid value for task array. {} must be a tigres.Task type".format(
                        type(t)))


class InputValues(TigresListType):
    """Values for inputs of a Task.

    :Example:

    >>> from tigres import InputValues
    >>> values = InputValues("my_values", [1, 'hello world'])
    """

    def __copy__(self):
        return self.__class__(self.name, self)


class InputArray(TigresListType):
    """Array of one or more InputValues, which will be inputs to a TaskArray in a Template.

    :Example:

    >>> from tigres import InputArray,InputValues
    >>> input_array = InputArray("input_array1", [InputValues("values",[1,'hello world'])])
    """


    def __init__(self, name=None, values=None):
        """Construct a new instance.

        :param name: User-defined name, e.g. "my tasks"
        :type name: str or None
        :param values: List of values in the array. This list is mutable.
        :type values: list
        """
        super(self.__class__, self).__init__(name, values)
        for t in self:
            if not is_tigres_object(InputValues, t):
                raise TypeError(
                    "Invalid value for input array. {} must be a tigris.InputValues or list type".format(
                        type(t)))


class InputTypes(TigresListType):
    """List of types for inputs of a Task.

    :Example:

    >>> from tigres import *
    >>> task1_types = InputTypes('Task1Types', [int, str])
    """

    def __repr__(self):
        return "{'name':'%s', 'types':%s}" % (self.name, self.__types_repr())

    def __str__(self):
        return "<%s.%s name:%s types:%s>" % (__name__, self.__class__.__name__,
                                             self.name, self.__types_repr())

    def __types_repr(self):
        return "[" + ', '.join([t.__name__ for t in self]) + "]"

    @property
    def types(self):
        return self


def get_number_args(impl):
    """
    Get the number of arguments for the given callable implementation
    :param impl: a callable implementation
    :return: number of arguments expected
    :raises: ValueError

    >>> def foo():
    ...     return "me"
    ...
    >>> class Foo(object):
    ...     def __call__(self):
    ...         return "me"
    ...
    >>> my_callable=Foo()
    >>> from tigres.core.task import get_number_args
    >>> get_number_args(foo)
    0
    >>> get_number_args(my_callable)
    0
    """

    validate_callable(impl)
    if isfunction(impl):
        arg_spec = getargspec(impl)
        number_args = len(arg_spec.args)
    else:
        arg_spec = getargspec(impl.__call__)
        number_args = len(arg_spec.args) - 1
    return number_args


TASK_ATTRIBUTES = [p for p in dir(Task) if not p.startswith('_')]


def is_task(obj):
    """
    Determines of the specified object is a `tigres.Task` using duck typing.
    :param obj:
    :return: True is it is a `tigres.Task`
    """
    for p in TASK_ATTRIBUTES:
        if not hasattr(obj, p):
            return False
    return True


def is_list(obj):
    """
    Determines of the specified object is a `list` using duck typing.
    :param obj:
    :return: True is it is a `list`
    """
    for p in LIST_ATTRIBUTES:
        if not hasattr(obj, p):
            return False
    return True


def is_tigres_object(tigres_class, tigres_obj):
    """
    Determine if the specified tigres object is a tigres class using duck typing
    :param tigres_class: The tigres class to validate the tigres object against
    :type tigres_class: class
    :param tigres_obj:
    :return:
    """
    if tigres_class in [InputArray, InputTypes, InputValues, TaskArray]:
        return is_list(tigres_obj)
    elif tigres_class is Task:
        return is_task(tigres_obj)
    else:
        raise ValueError(
            'Invalid tigres_class must be InputArray, InputTypes, InputValues, TaskArray or Task')


def validate_tigres_object(name, tigres_class, tigres_obj):
    """
    Validate that the given tigres object represents this given tigres_class
    :param name: name for the variable being validated
    :param tigres_class:
    :param tigres_obj:
    :return:
    """

    if tigres_obj and not is_tigres_object(tigres_class, tigres_obj):
        raise TypeError(
            "Invalid value for {} must be a {} type not {}".format(name,
                                                                   tigres_class.__name__,
                                                                   type(
                                                                       tigres_obj)))


def validate_callable(impl):
    """
    validate that the specified input is callable

    :param impl: the callable object to inspect
    :return: bool


    :Example:

    >>> def foo():
    ...     return "me"
    ...
    >>> class Foo(object):
    ...     def __call__(self):
    ...         return "me"
    ...
    >>> my_callable=Foo()
    >>> from tigres.core.task import validate_callable
    >>> validate_callable(foo)
    True
    >>> validate_callable(my_callable)
    True
    """
    if isinstance(impl, collections.Callable):
        return True
    else:
        raise ValueError("%s is not callable" % str(callable))







