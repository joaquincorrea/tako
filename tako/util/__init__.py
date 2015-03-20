"""
`tigres`
********

.. currentmodule:: tigres

:platform: Unix, Mac
:synopsis: The high level Tigres API. Initialization of Tigres templates, tasks and their inputs.
:module author: Dan Gunter <dkgunter@lbl.gov>, Gilberto Pastorello <gzpastorello@lbl.gov>, Val Hendrix <vhendrix@lbl.gov>


:Example:

>>> import tempfile as temp_file
>>> from tigres import *
>>> f = temp_file.NamedTemporaryFile()
>>> program=start(log_dest=f.name)
>>> set_log_level(Level.INFO)
>>> log_info("Tasks", message="Starting to prepare tasks and inputs")
>>> def adder(a, b): return a + b
>>> task1 = Task("Task 1", FUNCTION, adder)
>>> task2 = Task("Task 2", EXECUTABLE, "/bin/echo")
>>> tasks = TaskArray(None, [task1, task2])
>>> inputs = InputArray(None, [InputValues(None, [2, 3]), InputValues(None, ['world'])])
>>> log_info("Template", message="Before template run")
>>> sequence("My Sequence", tasks, inputs)
'world'
>>> # find() returns a list of records
>>> for o in find(activity="Tasks"):
...    assert o.message
>>> end()

Templates
----------

 * :func:`sequence`
 * :func:`parallel`
 * :func:`split`
 * :func:`merge`


Tasks and Inputs
-----------------

 * :class:`InputArray` - Array of one or more InputValues, which will be inputs to a TaskArray in a Template.
 * :class:`InputTypes` - List of types for inputs of a Task.
 * :class:`InputValues` - List of values for inputs matched to a task Task.
 * :class:`Task` - Function or program. A task is the atomic unit of execution in Tigres.
 * :class:`TaskArray` - List of one or more Tasks, which will be executed in a Template
 * :code:`EXECUTABLE` - identifies that a task implementation is an executable
 * :code:`FUNCTION` - identifies that a task implementation is a function


Initialization
---------------

Users should use :func:`start` to initialize the Tigres library, and this will initialize the logging as
well. When :func:`start` is called, the :code:`log_dest` keyword in that function is passed as the first
argument logger and any other keywords beginning in :code:`log_` will have the prefix stripped and
will be passed as keywords.

 * :func:`start`
 * :func:`end`
 * :func:`get_results`
 * :func:`set_log_level`




User logging
-------------

One of the design goals of the state management is to cleanly combine user-defined logs, e.g. about what
is happening in the application, and the automatic logs from the execution system.
The user-defined logging uses the familiar set of functions. Information is logged as a
Python dict of key/value pairs. It is certainly possible to log English sentences,
but breaking up the information into a more structured form will simplify querying for it later.

 * :func:`write`
 * :func:`log_error`
 * :func:`log_warn`
 * :func:`log_info`
 * :func:`log_debug`
 * :func:`log_trace`


Analysis
---------

 * :func:`check`
 * :func:`dot_execution` - write an execution graph for the currently running program
 * :class:`LogStatus`
 * :func:`find`
 * :func:`query`
 * :class:`Record`



Data dependencies
------------------

* :class:`PREVIOUS` - a syntax where the user can specify that the output of a previously executed task
  as input for future task.

    * handles dependencies between templates. ``PREVIOUS`` can be both implicit and explicit.
    * The semantics of ``PREVIOUS`` are affected by the template type.

---------------------

"""
from tigres.core.graph import dot_execution
from tigres.core.monitoring import start, end, log_error, log_info, log_trace, \
    log_warn, log_debug, write
from tigres.core.monitoring.log import set_log_level, check, find, query, LogStatus, \
    Level
from tigres.core.monitoring.kvp import Record, Keyword
from tigres.core.task import InputTypes, InputValues, InputArray, Task, \
    TaskArray, FUNCTION, EXECUTABLE
from tigres.core.template import sequence, parallel, split, merge
from tigres.core.template import get_results
from tigres.core.previous import PREVIOUS
from tigres.version import __version__

# Import functions and types for the core User API
__all__ = ['utils',
           'InputTypes', 'InputArray', 'InputValues', 'FUNCTION', 'EXECUTABLE',
           'PREVIOUS', 'TaskArray', 'Task',
           'sequence', 'merge', 'split', 'parallel',
           'log_debug', 'log_error', 'log_trace', 'log_debug', 'log_info',
           'log_warn',
           'set_log_level', 'start', 'end', 'write',
           'check', 'find', 'query', 'start', 'end', 'Level', 'get_results',
           'LogStatus', 'Record', 'Keyword', 'dot_execution']




