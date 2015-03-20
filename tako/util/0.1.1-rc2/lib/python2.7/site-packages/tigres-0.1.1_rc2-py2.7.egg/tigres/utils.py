"""
`tigres.utils`
**************

.. currentmodule:: tigres.utils

:platform: Unix, Mac
:synopsis: Tigres shared classes/methods/constants
:module author: Gilberto Pastorello <gzpastorello@lbl.gov>, Val Hendrix <vchendrix@lbl.gov>

Functions
=========
 * :py:func:`get_new_output_file` - Get a new unique output file name.

Classes
=========
 * :py:class:`Execution` - Execution plugins available for a Tigres program 
 * :py:class:`State` - States of a Tigres program
 * :py:class:`TigresException` - A Tigres Program Exception
 * :py:class:`TaskFailure` - Object to be returned in case of failures in task execution

"""
import os
import uuid


class Execution:
    """
    Execution plugins available

    * LOCAL_THREAD
    * LOCAL_PROCESS
    * DISTRIBUTE_PROCESS
    * SGE
    * SLURM

    """

    def __init__(self):
        pass

    LOCAL_THREAD = 'tigres.core.execution.plugin.local.ExecutionPluginLocalThread'
    LOCAL_PROCESS = 'tigres.core.execution.plugin.local.ExecutionPluginLocalProcess'
    DISTRIBUTE_PROCESS = 'tigres.core.execution.plugin.distribute.ExecutionPluginDistributeProcess'
    SGE = 'tigres.core.execution.plugin.job.ExecutionPluginJobManagerSGE'
    SLURM = 'tigres.core.execution.plugin.job.ExecutionPluginJobManagerSLURM'

    LOOKUP = {'EXECUTION_LOCAL_THREAD': LOCAL_THREAD,
              'EXECUTION_LOCAL_PROCESS': LOCAL_PROCESS,
              'EXECUTION_DISTRIBUTE_PROCESS': DISTRIBUTE_PROCESS,
              'EXECUTION_SGE': SGE,
              'EXECUTION_SLURM': SLURM
    }


    @classmethod
    def get(cls, name):
        """
        get the execution plugin for the name

        :Example:

        >>> from tigres.utils import Execution
        >>> Execution.get('EXECUTION_LOCAL_THREAD')
        'tigres.core.execution.plugin.local.ExecutionPluginLocalThread'

        :param name: execution plugin string
        :return: plugin module
        :rtype: str
        """
        return cls.LOOKUP[name]


class State:
    """
    Tigres State constants

    * NEW - NEW, CREATED or START
    * READY - WAIT, WAITING or READY
    * RUN - RUN, RUNNING or ACTIVE
    * DONE - DONE, TERMINATED or FINISHED
    * FAIL - FAIL or FAILED execution (e.g., from exception)
    * UNKNOWN - UNKNOWN

    """

    def __init__(self):
        pass

    NEW = 'NEW'  # NEW, CREATED or START
    READY = 'READY'  # WAIT, WAITING or READY
    RUN = 'RUN'  # RUN, RUNNING or ACTIVE
    BLOCKED = 'BLOCKED'  # BLOCKED by execution or data dependency
    DONE = 'DONE'  # DONE, TERMINATED or FINISHED
    FAIL = 'FAIL'  # FAIL or FAILED execution (e.g., from exception)
    UNKNOWN = '?'  # UNKNOWN

    _SEP = '_'  # Separator for joining with names

    @classmethod
    def paste(cls, name, state):
        """Combine a name and a state into a single string.
        """
        return cls._SEP.join((name, state))


def get_new_output_file(basename="tigres", extension="log"):
    """Get a new unique output file name.
    
    :param basename: Prefix for name
    :type basename: str
    :param extension: File suffix (placed after a '.')
    :type extension: str
    :return: Name of file (caller should open)
    """
    basedir = os.getcwd()
    identity = str(uuid.uuid4())
    if extension[0] == '.':  # some people don't read docs, so
        extension = extension[1:]  # be generous in what we accept
    return "{bd}/{bn}_{id}.{ext}".format(bd=basedir, bn=basename,
                                         id=identity, ext=extension)


class TigresException(Exception):
    """ A Tigres Program Exception"""
    pass


class TaskFailure(object):
    """Object to be returned in case of failures in task execution

    :param name: Name/description for error
    :type name: str
    :param error: Exception that occurred, if available
    :type error: Exception
    """

    def __init__(self, name='', error=TigresException('Task execution failed')):
        self.name = (name if name else 'Result from failed Task execution')
        self.error = error

    def __str__(self):
        if self.error:
            return "{} ({})".format(self.name, type(self.error).__name__)
        else:
            return self.name


