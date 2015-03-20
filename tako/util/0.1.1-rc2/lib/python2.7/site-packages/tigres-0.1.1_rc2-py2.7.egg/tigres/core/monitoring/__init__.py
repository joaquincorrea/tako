"""
`tigres.core.monitoring`
========================

.. currentmodule:: tigres.core.monitoring

:platform: Unix, Mac
:synopsis: The core tigres monitoring api


.. moduleauthor:: Dan Gunter <dkgunter@lbl.gov>

"""
from tigres.utils import Execution
from tigres.core.monitoring.log import write
from tigres.core.monitoring.common import Keyword, Level
from tigres.core.state.program import Program

__all__ = ['common', 'kvp', 'log', 'receive', 'search', 
           'write', 'log_debug', 'log_error',
           'log_trace', 'log_debug', 'log_info']

# # Syntactic sugar for write() at different levels and initialization of monitoring


def start(name=None, log_dest=None, execution=Execution.LOCAL_THREAD, **kwargs):
    """
    Configures and initializes tigres monitoring

    WARNING: this will end any previously started programs

    :param name:
    :param log_dest:
    :param name: Name of the program (Default: auto-generated name)
    :param log_dest:  The log file to write to. (Default: auto-generated filename)
    :param kwargs:
    :return: None
    :raises: TigresException
    """
    Program.clear()
    Program(name, log_dest=log_dest, execution=execution, **kwargs)


def end():
    """
   Clear the Tigres monitoring.
    :return: None
    """
    Program().clear()


def log_error(*args, **kwargs):
    """Write a user log entry at level ERROR.

    This simply calls `write()` with the `level` argument
    set to ERROR. See documentation of `write()` for details.
    """
    kwargs[Keyword.PROGRAM_UID] = Program().identifier
    return write(Level.ERROR, *args, **kwargs)


def log_warn(*args, **kwargs):
    """Write a user log entry at level ERROR.

    This simply calls `write()` with the `level` argument
    set to ERROR. See documentation of `write()` for details.
    """
    kwargs[Keyword.PROGRAM_UID] = Program().identifier
    return write(Level.WARN, *args, **kwargs)


def log_info(*args, **kwargs):
    """Write a user log entry at level INFO.

    This simply calls `write()` with the `level` argument
    set to INFO. See documentation of `write()` for details.
    """
    kwargs[Keyword.PROGRAM_UID] = Program().identifier
    return write(Level.INFO, *args, **kwargs)


def log_debug(*args, **kwargs):
    """Write a user log entry at level DEBUG.

    This simply calls `write()` with the `level` argument
    set to DEBUG. See documentation of `write()` for details.
    """
    kwargs[Keyword.PROGRAM_UID] = Program().identifier
    return write(Level.DEBUG, *args, **kwargs)


def log_trace(*args, **kwargs):
    """Write a user log entry at level TRACE.

    This simply calls `write()` with the `level` argument
    set to TRACE. See documentation of `write()` for details.
    """
    kwargs[Keyword.PROGRAM_UID] = Program().identifier
    return write(Level.TRACE, *args, **kwargs)


