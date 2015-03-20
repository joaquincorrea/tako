"""
`tigres.core.monitoring.common`
================================

.. currentmodule:: tigres.core.monitoring.common

:platform: Unix, Mac
:synopsis: Shared classes and settings for Tigres monitoring


"""
import logging
import threading

__author__ = 'Dan Gunter <dkgunter@lbl.gov>'
__date__ = '4/22/13'

import os

# : Default encoding for data in files
DEFAULT_ENCODING = 'utf-8'

#: Metadata line prefix character
META_LINE_MARKER = '#'

#: Env var for logging level
ENV_LOG_LEVEL = 'TIGRES_LOG_LEVEL'

LOG_FORMAT_JSON, LOG_FORMAT_NL = 'json', 'nl'


class NotInitializedError(Exception):
    """Raised when attempt to use API methods like query()
    before initializing Tigres or the monitoring API.
    """

    def __init__(self, caller):
        Exception.__init__(self,
                           "Must initialize monitoring API before calling {}()".format(
                               caller))


def env_log_level(default=None):
    """Find logging level, for Python logging, from
    the environment.

    :return: Level, or default
    :raise: Value error, if environ had unknown level
    """
    result = default
    try:
        elevel = os.environ[ENV_LOG_LEVEL]
        n = Level.to_number(elevel.upper())
        if n == Level.NONE:
            raise ValueError("Unknown level: {}".format(elevel))
        result = Level.to_logging(n)
    except KeyError:
        pass
    return result


class Level(object):
    """Defined levels, and some related functions.

    * NONE (0): Nothing; no logging
    * FATAL (10): Fatal errors
    * ERROR (20): Non-fatal errors
    * WARNING (30): Warnings, i.e. potential errors
    * INFO (40): Informational messages
    * DEBUG (50): Debugging messages
    * TRACE (60): More detailed debugging messages
    * Other levels up to 100 are allowed

    Note that the numeric equivalents of these levels is not the
    same as the Python logging package. Basically, they are in the
    reverse order -- higher levels are less fatal.
    The `Level` class has static methods for manipulating levels
    and converting to/from the Python logging levels.
    """
    NONE = 0
    FATAL = 10
    ERROR = 20
    WARNING = 30
    INFO = 40
    DEBUG = 50
    TRACE = 60
    MAX = 100

    WARN = WARNING

    _logging_map = {
        NONE: logging.NOTSET, FATAL: logging.FATAL, ERROR: logging.ERROR,
        WARN: logging.WARN, INFO: logging.INFO, DEBUG: logging.DEBUG,
        TRACE: logging.DEBUG - 1}

    _logging_num = {
        'FATAL': FATAL, 'ERROR': ERROR, 'WARN': WARNING, 'WARNING': WARNING,
        'INFO': INFO, 'DEBUG': DEBUG, 'TRACE': TRACE}

    _logging_name = {
        FATAL: 'FATAL', ERROR: 'ERROR', WARN: 'WARNING',
        INFO: 'INFO', DEBUG: 'DEBUG', TRACE: 'TRACE'}

    @staticmethod
    def names():
        """Return list of all names of levels, in no
        particular order.

        :return: List of level names
        :rtype: list of str
        """
        return list(Level._logging_num.keys())

    @staticmethod
    def to_logging(level):
        """Get Python logging module level.

        :param level: Tigres logging level
        :return: Equivalent Python logging module level.
        :rtype: int
        """
        return Level._logging_map.get(level, logging.NOTSET)

    @staticmethod
    def to_number(level_str):
        """Convert a string level to its numeric equivalent.

        :param level_str: String level (case-insensitive)
        :type level_str: str or basestring
        :return: Numeric level, or NONE
        :rtype: int
        """
        return Level._logging_num.get(level_str.upper(), Level.NONE)

    @staticmethod
    def to_name(levelno):
        """Convert a numeric level to its string equivalent.

        :param levelno: Numeric level
        :type levelno: int
        :return: String level, or empty string
        :rtype: str
        """
        return Level._logging_name.get(levelno, '')


class Keyword(object):
    # Netloggingger fields
    TIME = 'ts'
    LEVEL = 'level'
    EVENT = 'event'
    MESSAGE = 'message'
    # Tigres fields
    pfx = 'tg_'
    NAME = pfx + 'name'
    # STATE = pfx + 'state'
    STATE = EVENT
    NODETYPE = pfx + 'ntype'
    ERROR = pfx + 'error'
    ERROR_LOC = pfx + 'errloc'
    STATUS = pfx + 'status'
    MY_UID = pfx + 'id'
    TASK_UID = pfx + 'task_id'
    WORK_UID = pfx + 'work_id'
    TMPL_UID = pfx + 'template_id'
    TMPL_PID = pfx + 'template_pid'
    PROGRAM_UID = pfx + 'program_id'
    PROGRAM_NAME = pfx + 'program_name'
    THREAD_ID = pfx + 'thread_id'
    PROC_ID = pfx + 'proc_id'
    HOST_ID = pfx + 'host_id'
    TASK_NUM = pfx + 'task_num'
    INPUT = pfx + 'in'
    OUTPUT = pfx + 'out'
    # helpers

    @staticmethod
    def is_id(x):
        return x.endswith('_id')


class MetaKeyword(object):
    """Keywords in metadata lines"""
    ENCODING = 'encoding'
    FORMAT = 'format'


class NodeType(object):
    TASK = 'task'
     # parallel task internal to a template
    PARALLEL = 'parallel'
    # sequence task internal to a template
    SEQUENCE = 'sequence'
    TEMPLATE = 'template'
    PROGRAM = 'program'
    WORK = 'state'
    ANY = 'any'

    _all = (TASK, TEMPLATE, PROGRAM, WORK, ANY, PARALLEL, SEQUENCE)

    @classmethod
    def is_known(cls, name):
        return name in cls._all

    @classmethod
    def get_id(cls, name):
        return Keyword.pfx + name + '_id'

    @classmethod
    def known(cls):
        return cls._all


class ThreadVar:
    """
    Thread-safe/local access to a single variable value.
    """

    def __init__(self, local=True):
        self._lock = threading.Lock()
        if local:
            self._values = {}
        else:
            self._values = None
        self._value = None
        self._local = local

    def set(self, value):
        with self._lock:
            if self._local:
                self._values[self.tid()] = value
            else:
                self._value = value

    def get(self):
        with self._lock:
            if self._local:
                value = self._values.get(self.tid(), None)
            else:
                value = self._value
        return value

    def tid(self):
        return threading.current_thread().ident

