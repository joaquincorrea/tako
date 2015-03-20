"""

`tigres.core.previous`
***********************

.. currentmodule:: tigres.core.previous

:platform: Unix, Mac
:synopsis: Tigres PREVIOUS syntax

.. moduleauthor:: Val Hendrix <vhendrix@lbl.gov>


"""
import inspect

from tigres.core.state.program import Program
from tigres.core.utils import get_metaclass


class _PreviousMeta(type):
    """
        Metaclass for PREVIOUS class to return an attribute at the class level.
    """

    # special attributes
    I = "i"

    def __getattr__(cls, item):
        """
        Class level get attribute. Determines if
        the item is a local variable.

        :param item: the attribute that represents a task or template
        :return: new instance of PREVIOUS
        """

        previous = PREVIOUS()
        if item == _PreviousMeta.I:
            # sets _i to True
            # noinspection PyStatementEffect,PyStatementEffect
            previous.i
        else:
            previous.__dict__['_work_name'] = item

            # Find the task in the local variables
            _, _, _, localz = inspect.getargvalues(
                inspect.currentframe().f_back)
            named_variable = cls._find_named_variable(localz, item)
            # need to go back one frame to see if we can find the variable name
            if not named_variable:
                # noinspection PyBroadException
                try:
                    _, _, _, localz = inspect.getargvalues(
                        inspect.currentframe().f_back.f_back)
                    named_variable = cls._find_named_variable(localz, item)
                except:
                    #do nothing
                    pass
            if named_variable:
                previous.__dict__['_work'] = localz[item]
                previous.__dict__['_work_name'] = localz[item].unique_name
            else:
                previous.__dict__['_work'] = None
        return previous

    @staticmethod
    def _find_named_variable(localz, variable_name):
        """
        Inspect python code for the variable name

        :param localz: The localz to inspect for the variable
        :param localz: ArgsSpec
        :param variable_name: The name of the variable to look for
        """
        if variable_name in localz:
            if hasattr(localz[variable_name], 'name'):
                return localz[variable_name]
        return None


class PreviousSyntaxError(Exception):
    pass


class PREVIOUS(get_metaclass(_PreviousMeta, 'PREVIOUSMeta')):
    """
    :class:`PREVIOUS` is a syntax where the user can specify that the output of a previously
    executed task as input for a task. :code:`PREVIOUS`  creates dependencies between templates
    and can be both implicit and explict. 

     * ``PREVIOUS``: Use the entire output of the previous task as the input.
     * ``PREVIOUS.i``: sed to split outputs across parallel tasks from the previous task.It matches the i-th output of
       | the previous task/template to the i-th InputValues of the task to be run. This only works for parallel tasks.
     * ``PREVIOUS.i[n]``: Use the n-th output of the previous task or template as input.
     * ``PREVIOUS.taskOrTemplateName``: Use the entire output of the previous template/task with specified name.
     * ``PREVIOUS.taskOrTemplateName.i``: Used to split outputs from the specified task or template across parallel tasks.
       | Match the i-th output of the previous task/template to the i-th InputValues of the task to be run.  This only works for parallel tasks.
     * ``PREVIOUS.taskOrTemplateName.i[n]`` Use the n-th output of the previous task or template as input.



    """

    def __init__(self):

        # Inspect the stack
        # Raise exception if instantiated outside of tigres.core
        frm = inspect.stack()[1]
        mod = inspect.getmodule(frm[0])
        if not mod.__name__.startswith("tigres.core"):
            raise PreviousSyntaxError("PREVIOUS may not be instantiated")

        # Initialize variables
        self._i = False
        self._index = None
        self._task = None
        self._work_name = None

    def _get_work(self, program):
        work_list = program.get_work_by_name(self._work_name)
        if len(work_list) > 1:
            # Don't know what to do with multiple instances of this name.
            raise PreviousSyntaxError(
                "Ambiguous inputs for {0:s}. There are {1:d} to choose from.".format(
                    str(self), len(work_list)))
        return program._work[work_list[0]]

    def __call__(self, index=0):
        """
        Return the previous results
        """
        # Get the Tigres Program
        program = Program()
        if not self._work_name and not self._i:
            # Syntax: PREVIOUS
            return program.previous_work.results
        elif self._work_name and not self._i:
            # Syntax: PREVIOUS.task_name
            return self._get_work(program).results
        elif not self._work_name and self._i:
            if self._index is not None:
                # Syntax: PREVIOUS.i[n]
                return program.previous_work.results[self._index]
                # Syntax: PREVIOUS.i
            else:
                if len(program.previous_work.results) < index + 1:
                    raise PreviousSyntaxError(
                        "PREVIOUS.{{name}}.i ERROR - cannot find the input for index {0:d} and previous state  {1:s}".format(
                            index,
                            program.previous_work.name))
                return program.previous_work.results[index]
        elif self._work_name and self._i:
            if self._index is not None:
                # Syntax: PREVIOUS.task_name.i[n]
                return self._get_work(program).results[self._index]

            else:
                # Syntax: PREVIOUS.task_name.i
                prev_work = self._get_work(program)
                if len(prev_work.results) < index + 1:
                    raise PreviousSyntaxError(
                        "PREVIOUS.{{name}}.i ERROR - cannot find the input for index {0:d} and previous state  {1:s}".format(
                            index,
                            prev_work.name))
                return self._get_work(program).results[index]

    def __str__(self):
        s = "PREVIOUS"
        if self._work_name:
            s += "." + self._work_name
        if self._i:
            s += ".i"
        if self._index:
            s += "[{}]".format(self._index)

        return s

    __repr__ = __str__

    def __getattr__(self, item):
        """
        Get attribute sets attribute values and return the PREVIOUS object

        :param item: attribute to set
        :return: self
        :rtype: PREVIOUS
        """
        if item == _PreviousMeta.I:
            if self._i:
                raise PreviousSyntaxError(
                    "Invalid use of .i syntax for {}".format(str(self)))
            self._i = True
        else:
            raise PreviousSyntaxError(
                "Invalid use of PREVIOUS syntax for {}. You may not specify a name here.".format(
                    str(self)))
        return self

    def __getitem__(self, index):
        """
        Special case of __getitem__ for PREVIOUS syntax.  This will set the index to be used for PREVIOUS semantics.

        :param index:
        :raise:
        """
        if not self._i:
            raise PreviousSyntaxError(
                ("Invalid use of PREVIOUS syntax for {}. An"
                 "index can only be specified after i. (e.g. PREVIOUS.i[5])"
                ).format(str(self)))
        if not isinstance(index, int):
            raise PreviousSyntaxError(("Invalid use of PREVIOUS syntax for {}. "
                                       "Index must be an integer."
                                      ).format(str(self)))
        self._index = index
        return self