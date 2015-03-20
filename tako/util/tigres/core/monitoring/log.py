"""
`tigres.core.monitoring.log`
============================

.. currentmodule:: tigres.core.monitoring.log

:platform: Unix, Mac
:synopsis: The core tigres monitoring log api



.. moduleauthor:: Dan Gunter <dkgunter@lbl.gov>


"""
from collections import OrderedDict

__date__ = "3/25/13"
try:
    xrange
    range = xrange
except NameError:
    pass

# Standard imports
from datetime import datetime
import itertools
import json
import logging
import os

logging.addLevelName(logging.DEBUG - 1, "TRACE")

try:
    import queue
except ImportError:
    import Queue

    queue = Queue
import re
import signal
import socket
import sqlite3

try:
    from  urllib import parse
except ImportError:
    import urlparse

    parse = urlparse
import threading
import time
from uuid import uuid4

# Package imports
from tigres.utils import State
from tigres.core.monitoring.kvp import Reader, Record, LogWriter, KvpFormatter
from tigres.core.monitoring.receive import TCPClient
from tigres.core.monitoring.common import NotInitializedError
from tigres.core.monitoring.common import Level, Keyword, MetaKeyword, NodeType
from tigres.core.monitoring.common import DEFAULT_ENCODING, META_LINE_MARKER
from tigres.core.monitoring.common import LOG_FORMAT_JSON, LOG_FORMAT_NL
from tigres.core.monitoring import search

# Global internal log objs
_log, _ulog = None, None
# Global stop sign, and ack
g_stop_now, g_stop_ack = False, True
STOP_TIMEOUT = 2  # seconds

_log_readonly = False

# Default level
_tigres_level = Level.INFO

# Log format
_tigres_log_format = LOG_FORMAT_JSON

# Tigre Program Variables
_program_name = None
_program_uuid = None


def get_log_format():
    return _tigres_log_format


def set_log_format(s):
    global _tigres_log_format
    try:
        s = s.lower()
    except AttributeError:
        raise ValueError("Log format must be a string")
    formats = (LOG_FORMAT_JSON, LOG_FORMAT_NL)
    if s not in formats:
        raise ValueError("Log format must be {}".format(' or '.join(formats)))
    _tigres_log_format = s


def _safe_del(d, keys):
    for k in keys:
        if k in d:
            del d[k]


# Some helper code to get host address

_host_address_cached = None


def _get_host_address_timeout(signo, frame):
    raise Exception("Hostname timeout")


def get_host_address():
    """Find IP address for this host.

    This uses signals to do a quicker timeout. Please don't try
    to call this in a multi-threaded context.

    :return: IP address
    :rtype: str
    """
    global _host_address_cached
    if _host_address_cached is not None:
        return _host_address_cached
    if os.getenv("PBS_ENVIRONMENT") is not None:
        # get it from PBS
        pass  # .. TODO:: get from PBS env vars
        # otherwise, get IP addr
    signal.signal(signal.SIGALRM, _get_host_address_timeout)
    # . start with hostname from fqdn()
    signal.alarm(2)
    # noinspection PyBroadException
    try:
        hostname = socket.getfqdn()
    except:
        hostname = '*'
    signal.alarm(0)
    # . get IP from host name
    ip = hostname
    try:
        ip = socket.gethostbyname(hostname)
    except socket.gaierror:
        # DNS lookup failed; try to get name of default outgoing interface
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(("google.com", 80))
            hostname = s.getsockname()[0]
            ip = socket.gethostbyname(hostname)
        except socket.gaierror:
            # give up, will just use hostname
            if ip == '*':
                ip = '0.0.0.0'
    _host_address_cached = ip
    return ip


def set_host_address(addr):
    """Put fixed value into host address.
    """
    global _host_address_cached
    _host_address_cached = addr


# end of helper code


class BaseLogger(object):
    """Base class for log output streams.
    """
    is_file = False

    def set_level(self, level):
        pass

    def log(self, level, name, kvp):
        pass

    def close(self):
        pass


class FileLogger(BaseLogger, KvpFormatter):
    """Send logs to a file (using Python logging).
    """

    def __init__(self, path, logger_name, mode='a'):
        """Initialize Python Logger output for monitoring data.
        """
        KvpFormatter.__init__(self, get_log_format())
        self.path = path
        self.is_file = True

        self._log = logging.getLogger(logger_name)
        self._handler = logging.FileHandler(path, encoding=DEFAULT_ENCODING,
                                            mode=mode)
        self._meta_formatter = JsonFormatter(meta=True)
        if self._is_json:
            self._main_formatter = JsonFormatter()
        else:
            self._main_formatter = LogFormatter()
            # self._meta_formatter = logging.Formatter("{} %(msg)s".format(META_LINE_MARKER))
        self._handler.setFormatter(self._main_formatter)
        if not self._log.handlers:
            self._log.addHandler(self._handler)

        self.set_level(Level.INFO)

        readonly = mode == 'r'
        if not readonly:
            self.add_metadata({MetaKeyword.ENCODING: DEFAULT_ENCODING,
                               MetaKeyword.FORMAT: get_log_format()})

    def add_metadata(self, kvp):
        """Write some metadata into the log.
        """
        self._handler.setFormatter(self._meta_formatter)
        self._log.log(self._loglevel, json.dumps(kvp)[1:-1])
        self._handler.setFormatter(self._main_formatter)

    def set_level(self, level):
        self._loglevel = Level.to_logging(level)
        self._log.setLevel(self._loglevel)

    def log(self, level, name, kvp):
        """Log the information

        :return: Whether anything was logged (due to levels)
        :rtype: bool
        """
        did_log = False
        kvp[Keyword.NAME] = name
        logging_level = Level.to_logging(level)
        if self._log.isEnabledFor(logging_level):
            self._log.log(logging_level, self._kvp_str(kvp))
            did_log = True
        return did_log

    def close(self):
        """Close the logger.
        """
        if self._log is not None:
            for hndlr in self._log.handlers:
                self._log.removeHandler(hndlr)
                hndlr.close()

            del self._log
            self._log = None


class SqliteLogger(BaseLogger):
    """Send logs to SQLite file.
    """
    TABLE = 'log'
    KEY_COL, VAL_COL, ID_COL = 'key', 'value', 'item'

    def __init__(self, path):
        """Initialize SQLite3 output for monitoring data.
        """
        self._level = Level.INFO
        self._insert_stmt = '''insert into {table}({lid}, {key}, {val}) values (?, ?, ?)''' \
            .format(table=self.TABLE, key=self.KEY_COL, val=self.VAL_COL,
                    lid=self.ID_COL)
        self._log_num = 0
        self._queue = queue.Queue()
        self._processing_thread = threading.Thread(target=self._process,
                                                   args=(path,))
        self._processing_thread.start()

    def set_level(self, level):
        self._level = level

    def log(self, level, name, kvp):
        if level > self._level:
            return
        kvp[Keyword.NAME] = name
        self._queue.put(list(kvp.items()))

    def close(self):
        """Close the logger.
        """
        del self._db
        self._db = None

    def _process(self, path):
        """Connect to DB and insert items from queue.

        Connecting to DB and inserts must be from same thread.
        """
        global g_stop_ack
        g_stop_ack = False
        self._db = sqlite3.connect(path)
        self._db.execute('''CREATE TABLE IF NOT EXISTS {table}
                  (id INTEGER PRIMARY KEY autoincrement, {lid} INTEGER, {key} TEXT, {val} TEXT);
                  '''.format(table=self.TABLE, key=self.KEY_COL,
                             val=self.VAL_COL, lid=self.ID_COL))
        while not g_stop_now:
            items = self._queue.get(block=True, timeout=STOP_TIMEOUT)
            if len(items) > 0:
                n = self._log_num
                self._db.executemany(self._insert_stmt,
                                     ((n, k, v) for k, v in items))
                self._db.commit()
                self._log_num += 1
        g_stop_ack = True


class NullMutex(object):
    """Null pattern for a mutex"""

    def acquire(self):
        return True

    def release(self):
        return


class SocketLogger(BaseLogger):
    """Send log messages over a socket.

    Yes, there is a SocketHandler in the logging package, but it is a
    dumb-ss about forcing every message to be pickled and unpickled -- much easier
    to just write the messages in a stream over a socket.
    """

    def __init__(self, host=None, port=None, mutex=None):
        """Connect to server.

        :param host: Server host, see TCPClient `host` param
        :param port: Server port, see TCPClient `port` param
        :param mutex: Mutex to use for thread-safety, may be None
        :raise: See TCPClient, also ValueError if port is not an int
        """
        kwd = {}
        if host:
            kwd['host'] = str(host)
        if port:
            kwd['port'] = int(port)
        self._client = TCPClient(**kwd)
        self._level = Level.INFO
        self._fmt = LogWriter(fmt=get_log_format())
        self._mtx = mutex if mutex else NullMutex()

    def set_level(self, level):
        self._level = level

    def log(self, level, name, kvp):
        if level > self._level or self._client is None:
            return
        kvp[Keyword.NAME] = name
        kvp[Keyword.LEVEL] = self._level
        kvp[Keyword.TIME] = time.time()
        self._mtx.acquire()
        try:
            s = self._fmt.format(kvp)
            self._client.send(s)
        finally:
            self._mtx.release()

    def close(self):
        """Close the logger.
        """
        if self._client is None:
            return
        self._mtx.acquire()
        try:
            del self._client
            self._client = None
        finally:
            self._mtx.release()


class LogFormatter(logging.Formatter):
    def __init__(self):
        logging.Formatter.__init__(self,
                                   "{}=%(asctime)s level=%(levelname)s %(message)s"
                                   .format(Keyword.TIME))

    def formatTime(self, record, *args):
        return datetime.now().isoformat()


class JsonFormatter(logging.Formatter):
    def __init__(self, meta=False):
        tmpl = '{{"{}": "%(asctime)s", "level": "%(levelname)s", %(message)s}}'
        if meta:
            tmpl = "{} {}".format(META_LINE_MARKER, tmpl)
        logging.Formatter.__init__(self, tmpl.format(Keyword.TIME))

    def formatTime(self, record, *args):
        return datetime.now().isoformat()


class UUID:
    """UUID wrapper.
    """
    symbols = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
    base = len(symbols)

    def __init__(self):
        self._u = uuid4()

    def encode(self, short=False):
        if short:
            digits, (num, rem) = [], divmod(self._u.int, self.base)
            while num:
                digits.insert(0, self.symbols[rem])
                num, rem = divmod(num, self.base)
            s = self.symbols[rem] + ''.join(digits)
        else:
            s = str(self._u)
        return s

    def __str__(self):
        return self.encode()

    @classmethod
    def u(cls):
        return str(cls())


def _create_name(prefix='', suffix='', body=''):
    name_format = ''.join(
        [('', '{prefix}.')[bool(prefix)],
         '{body}',
         ('', '.{suffix}')[bool(suffix)]])
    if not body:
        body = UUID().encode(short=True)
    name = name_format.format(prefix=prefix, body=body, suffix=suffix)
    return name


def _dict_append(d, key, value):
    """Append a value to a dictionary.
    Create new list with one value if there is no value for the key.

    :return: Input dictionary (for chaining).
    """
    cur = d.get(key, None)
    if cur:
        cur.append(value)
    else:
        d[key] = [value]
    return d


def _init_logger(dest, name, readonly=None):
    """Called by public init() method.
    """
    parts = parse.urlparse(dest)
    if parts.scheme == 'sqlite':
        log = SqliteLogger(parts.path)
    elif parts.scheme == 'tcp':
        log = SocketLogger(host=parts.hostname, port=parts.port,
                           mutex=threading.Lock())
    else:
        path = None
        if os.name == 'nt':
            path = dest
        elif parts.scheme in ('', 'file'):
            path = parts.path

        if path:
            log = FileLogger(path, name, mode='r' if readonly else 'a')
        else:
            raise ValueError("unknown destination URL format: {}".format(dest))
    return log


def log_task(name, state, level=Level.INFO, **kwargs):
    """Log information about a task, with addition identifiers and such
    passed in `kwargs`.

    :param name: Name of the task
    :type name: str
    :param state: State of the task (e.g. RUN)
    :type state: str, enumeration defined in shared.State class
    :param kwargs: Additional information about the task
    :return: None
    """
    return log_node(level, name, state=state, nodetype='task', **kwargs)


def log_template(name, state, type_='', level=Level.INFO, **kwargs):
    """Log information about a template.
    """
    ttype = 'template_' + type_
    return log_node(level, name, state=state, nodetype=ttype, **kwargs)


def log_node(level, name, state=None, nodetype='any', **kwargs):
    """Log information for node.

    :return: Whether anything was actually logged
    :rtype: bool
    """
    if _log is None:
        return False
    tname = name.replace(' ', '+')
    kwargs[Keyword.NODETYPE] = nodetype
    if state:
        kwargs[Keyword.STATE] = state
    if Keyword.PROGRAM_UID not in kwargs:
        kwargs[Keyword.PROGRAM_UID] = _program_uuid
    if Keyword.PROGRAM_NAME not in kwargs:
        kwargs[Keyword.PROGRAM_NAME] = _program_name
    return _log.log(level, tname, kwargs)


class Metadata(dict):
    """A dict with a few logging-related convenience methods.
    """

    def __init__(self, **kwargs):
        dict.__init__(self, kwargs)
        self[Keyword.STATUS] = 0

    def set_uuid(self, nodetype):
        """Create and set a new UUID, for a given type of node.

        :param nodetype: Type of node
        :return: The new UUID
        """
        uuid = UUID.u()
        self[NodeType.get_id(nodetype)] = uuid
        return uuid

    def get_uuid(self, nodetype):
        return self[NodeType.get_id(nodetype)]

    def set_host_context(self):
        """Populate values for the current thread, process, and host.
        """
        self[Keyword.THREAD_ID] = threading.current_thread()
        self[Keyword.PROC_ID] = os.getpid()
        self[Keyword.HOST_ID] = get_host_address()

    def set_error(self, err, loc=None, code=-1):
        """Set metadata for an exception.
        """
        self[Keyword.STATUS] = code
        errmsg = str(err)
        self[Keyword.ERROR] = errmsg
        if loc is not None:
            self[Keyword.ERROR_LOC] = loc

    def add_inputs(self, values):
        """Add inputs to the logged information.

        :param values: List of input values
        :type values: tigres.type.InputValues
        :return: None
        """
        key = "{}_num".format(Keyword.INPUT)
        self[key] = len(values)


class LogStatus(object):
    """Provide convenient access to status information related to
    a single log entry.

    Instances of this class are returned by the `check()` function.
    """

    def __init__(self, rec):
        """Constructor.

        :param rec: Record
        :type rec: kvp.Record
        """
        self._rec = rec
        self.timestr = rec.time_string  # : String representation of time
        self.timestamp = rec.ts  # : Numeric time in seconds since 1/1/1970
        self.state = rec.get(Keyword.STATE, State.UNKNOWN)  # : Activity or state
        self.errcode = int(rec.get(Keyword.STATUS, 0))  # : Error code, 0=OK
        self.errmsg = rec.get(Keyword.ERROR,
                              None)  # : Error message. None if no error
        self.name = rec.get(Keyword.NAME,
                            'UNKNOWN').replace("+", " ")  # : Name of node, or 'user' for user-created log entries
        self.template_id = rec.get(Keyword.TMPL_UID,
                                   None)  # : Identifier for the current template. May be None
        self.task_id = rec.get(Keyword.TASK_UID,
                               None)  # : Identifier for the current task. May be None
        self.work_id = rec.get(
            Keyword.WORK_UID)  # : Identifier for the current state being don
        self.program_id = rec.get(Keyword.PROGRAM_UID,
                                   None)  # : Identifier for the current program. May be None
        self.program_name = rec.get(Keyword.PROGRAM_NAME,
                                   None)  # : Name for the current program. May be None
        self.node_type = rec.get(Keyword.NODETYPE,
                                   None)  # : Node type. May be None
        self.meta = {k: v for k, v in list(rec._fields.items()) if
                     not k.startswith(Keyword.pfx)}

    def __getitem__(self, key):
        """Get a value from the record.

        :param key: Field name in the record
        :return: The value or an empty string if not found
        """
        return self.meta.get(key, '')

    def to_json(self):
        """Return JSON representation of status.
        """
        d = dict(timestr=self.timestr, ts=self.timestamp,
                 state=self.state, name=self.name,
                 errcode=self.errcode, errmsg=self.errmsg)
        d.update(self.meta)
        return d

    def __str__(self):
        """Calls to_json().
        """
        d = self.to_json()
        return json.dumps(d)

        # sugar


class BuildQueryError(Exception):
    """Indicates that there was trouble building the query.
    """
    pass


# ############################################################################################
# High-level API
#############################################################################################


def init(dest, program_name='', program_uuid='', user_dest=None,
         format=LOG_FORMAT_NL, readonly=False,
         host='localhost'):
    """Initialize Tigres monitoring environment.

    This will open the output destinations and write any header(s) to them.

    NOTE: You shouldn't call this function explicitly if you called
    the top-level tigres `init()` with a log destination keyword.

    :param dest: Destination URL for Tigres logs.
    :param user_dest: Destination URL for user-generated logs.
        If not given, `dest` will be used for both.
    :param format: Format for file logs, one of the LOG_FORMAT_* constants
    :type format: str
    :param host: User-provided host addr (skip DNS lookup)
    :type host: str
    :return: None
    :raise: ValueError if a dest URL is not recognized, or `format` is invalid

    :Example:

    >>> from tigres.core.monitoring import log
    >>> log.init("myfile.log")

    """
    global _log, _ulog, _log_readonly, _host_address_cached, _program_name, _program_uuid
    _program_name = program_name
    _program_uuid = program_uuid
    if host is not None:
        set_host_address(host)
    set_log_format(format)
    if _log is not None or _ulog is not None:
        finalize()
    if not dest:
        raise ValueError("destination URL cannot be empty")
    _log = _init_logger(dest, 'tigres', readonly=readonly)
    if user_dest:
        _ulog = _init_logger(user_dest, 'tigres.user')
    else:
        _ulog = _log
    _log_readonly = readonly


def finalize():
    """Shut down Tigres monitoring.

    It is safe to call this multiple times.

    :rtype : object
    NOTE: You shouldn't call this function explicitly if you called
    the top-level tigres `start()` with a log destination keyword.
    The top-level `end()` call will invoke this function for you.
    """
    global g_stop_now, _log, _ulog
    if _log is None and _ulog is None:
        return
    g_stop_now = 1
    for i in range(10 * STOP_TIMEOUT + 1):
        if g_stop_ack:
            break
        time.sleep(0.1)
        # Write a final message
    # Close the logs
    if _ulog is not _log:
        _ulog.close()
    _log.close()
    _ulog = None
    _log = None


def set_log_level(level):
    """Set the level of logging for the user-generated logs.

    After this call, all messages at a numerically higher level
    (e.g. DEBUG if level is INFO) will be skipped.

    :param level: One of the constants defined in this module:
        NONE, FATAL, ERROR, WARN[ING], INFO, DEBUG, TRACE
    :type level: int
    :return: None
    :raise: ValueError if `level` is out of range or not an integer


    """
    if _ulog is None:
        return
    if isinstance(level, str):
        level = Level.to_number(level)
    if not isinstance(level, int) or not Level.NONE <= level <= Level.MAX:
        raise ValueError("level {} out of range {:d} .. {:d}".
                         format(level, Level.NONE, Level.MAX))
    _ulog.set_level(level)


def write(level, activity, message=None, **kwd):
    """Write a user log entry.

    If the API is not initialized, this does nothing.

    :param level: Minimum level of logging at which to show this
    :param activity: What you are doing
    :param message: Optional message string, to enable simple message-style logging
    :param kwd: Contents of log entry. Note that timestamp is automatically added.
        Do not start keys with `tg_`, which is reserved for Tigres, unless you know what you are doing.
    :return: None

    Example::

        # Write a simple message
        write(Level.WARN, "looking_up", "I see a Vogon Constructor Fleet")
        # Or, using the more convenient form
        warn("looking_up", "I see a Vogon Constructor Fleet")
        # Use key/value pairs
        warn("looking_up", vehicle="spaceship", make="Vogon", model="Constructor", count=42)
    """
    if _ulog is None:
        return
    kwd[Keyword.EVENT] = activity
    if message is not None:
        kwd[Keyword.MESSAGE] = message
    if Keyword.PROGRAM_UID not in kwd:
        kwd[Keyword.PROGRAM_UID] = _program_uuid
    if Keyword.PROGRAM_NAME not in kwd:
        kwd[Keyword.PROGRAM_NAME] = _program_name

    # Log the information
    _ulog.log(level, 'user', kwd)


def check(nodetype, **kwargs):
    """Get status of a task or template (etc.).

    :param nodetype: Type of thing you are looking for
    :type nodetype: str, See ``NodeType`` for defined values
    :param state: Only look at nodes in the given state (default=DONE, None for any)
    :type state: str
    :param multiple: If True, return all matching items; otherwise group by nodetype and, if given, name.
                     Return only the last record for each grouping.
    :type multiple: bool
    :param names: List of names (may be regular expressions) of nodes to look for.
    :type names: list of str, or str
    :param program_id: Only checks the nodes for the specifed program
    :param template_id: only checks the nodes that belong to the template with
            the specified template_id.

    :return: list(LogStatus). See parameter `multiple`.
    """
    if _log is None:
        raise NotInitializedError("check")
    if not NodeType.is_known(nodetype):
        raise ValueError(
            "Unknown nodetype {}, not in {}".format(nodetype, NodeType.known()))
    node_id_key = NodeType.get_id(nodetype)
    check_nodetype = nodetype != NodeType.ANY
    check_state = 'state' in kwargs and kwargs['state'] is not None
    multiple = False
    in_template_id = None
    in_program_id = None
    if 'template_id' in kwargs:
        in_template_id = kwargs['template_id']
    if 'program_id' in kwargs:
        in_program_id = kwargs['program_id']
    if not in_program_id:
        in_program_id = _program_uuid
    if 'multiple' in kwargs:
        multiple = kwargs['multiple']
    name_filters = []
    names = None
    if 'names' in list(kwargs.keys()):
        names = kwargs['names']
        if isinstance(names, str):
            names = [names]
        if not names:
            names = ['']
        for name in names:
            if not name.startswith('^'):
                name = '.*' + name
            name_filters.append(re.compile(name))

    def fltr(rec):

        if in_program_id and not in_program_id == rec.get(Keyword.PROGRAM_UID, None):
            return False
        if rec.get(Keyword.NODETYPE, None) is None or rec.get(Keyword.NAME, None) is None:
            return False
        if check_nodetype and not rec.get(Keyword.NODETYPE, None).startswith(nodetype):
            return False
        if in_template_id and not in_template_id.replace(" ", "+") == rec.get(Keyword.TMPL_UID, None):
            return False
        if check_state:
            log_state = rec.get(Keyword.STATE, None)
            if log_state is not None and kwargs['state'] != log_state:
                return False
        if name_filters:
            return any([nf.match(rec.get(Keyword.NAME, "")) for nf in name_filters])
        else:
            return True

    # Let's keep the results in order that the
    # nodes are created in (i.e. when they are logged as new)
    result = OrderedDict()
    keylist = [Keyword.NODETYPE]
    keylist.append(Keyword.NAME)
    keylist.append(node_id_key)
    if _log and _log.is_file:  # XXX: Put this logic in BaseLogger subclasses
        with open(_log.path, 'rb') as f:
            linenum = 1
            try:
                for rec in Reader(f):
                    if fltr(rec):
                        key = '#'.join([rec.get(k, '') for k in keylist])
                        if multiple:
                            # keep a list per name
                            _dict_append(result, key, rec)
                        else:
                            # keep only 1 per name
                            result[key] = rec
                    linenum += 1
            except ValueError as err:
                # add line number to error before re-raising
                raise ValueError("Line {:d}: {}".format(linenum, err))
    else:
        raise NotImplementedError(
            'Only know how to search files, not {}'.format(_log.path))
    if result:
        if multiple:
            flattened = itertools.chain.from_iterable(iter(result.values()))
            return list(map(LogStatus, flattened))
        else:
            return list(map(LogStatus, list(result.values())))
    else:
        return None


def find(name=None, states=None, activity=None, template_id=None, task_id=None,
         **key_value_pairs):
    """Find and return log records based on simple criteria of the name
    of the activity and matching attributes (key/value pairs).

    :param name: Name of node, may be empty
    :param states: List of user activities and/or Tigres states to snippets, None being "all".
    :type states: list of str or None
    :param activity: One specific user activity (may also be listed in 'states')
    :type activity: str
    :param template_id: Optional template identifier to filter against
    :type template_id: str
    :param task_id: Optional task identifier to filter against
    :type task_id: str
    :param key_value_pairs: Key-value pairs to match against. The value may be a number
           or a string. Exact match is performed. If a key is present in the record, the value must match.
           If a key is not present in a record, then it is ignored.
    :return: List of `Record` instances representing the original user log data
    """
    if _log is None:
        raise NotInitializedError("find")
    result, ignore_fields = [], []
    if _program_uuid:
        key_value_pairs[Keyword.PROGRAM_UID] = _program_uuid
    if template_id:
        key_value_pairs[Keyword.TMPL_UID] = template_id
    if task_id:
        key_value_pairs[Keyword.TASK_UID] = task_id
    if key_value_pairs:
        kvp_rec = Record({k: str(v) for k, v in key_value_pairs.items()})
        # ignore auto-generated fields in the record, that did not have
        # a value in the key-value pairs
        ignore_fields = list(
            Record.AUTO_FIELDS.difference(key_value_pairs.keys()))
    else:
        kvp_rec = None
    if states:
        log_states = dict.fromkeys(states, True)
    else:
        log_states = {}
    if activity:
        log_states[activity] = True
    if _log and _log.is_file:  # XXX: Put this logic in BaseLogger subclasses
        reader = Reader(_log.path)
        for rec in reader:
            # check name
            if name and not (rec.get(Keyword.NAME, None) == name):
                continue
                # check states
            if log_states:
                log_state = rec.get(Keyword.STATE, None)
                if not log_state in log_states:
                    continue
                    # check kvp
            match = (not kvp_rec) or rec.intersect_equal(kvp_rec,
                                                         ignore=ignore_fields)
            if match:
                result.append(rec)
    else:
        raise NotImplementedError(
            'Only know how to search files, not {}'.format(_log.path))
    return result


def query(spec=None, fields=None):
    """Find and return log records based on a list of filter expressions.

    Supported operators for `spec` are:

    * >=  :   Compare two numeric values, A and B, and return whether A >= B.
    * =   :   Compare two numeric or string values, A and B, and return whether A = B.
    * <=  :   Compare two numeric values, A and B, and return whether A <= B.
    * >   :   Compare two numeric values, A and B, and return whether A > B.
    * !=  :   Compare two numeric or string values, A and B, and return whether A != B.
    * <   :   Compare two numeric values, A and B, and return whether A < B.
    * ~   :   Compare the string value, A, with the regular expression B, and return whether B matches A.

    :Example expressions:

       `foo > 1.5` -- find records where field foo is greater than 1.5, ignoring records
       where foo is not a number.

       `foo ~ 1\.\d` -- find records where field foo is a '1' followed by a decimal point
       followed by some other digit.

    :param spec: Query specification, which is a list of expressions of the form
                 "name op value". See documentation for details.
    :type spec: list of str
    :param fields: The fields to return, in matched records. If empty, return all
    :type fields: None or list of str
    :return: Generator function. Each item represents one record.
    :rtype: list of Record
    :raise: BuildQueryError, ValueError
    """
    if not spec: spec = []
    if _program_uuid:
        spec.append("{} = {}".format(Keyword.PROGRAM_UID, _program_uuid))

    if not fields: fields = []
    if _log is None:
        raise NotInitializedError("query")
    try:
        clause = search.Clause(exprs=list(map(search.Expr, spec)))
        qry = search.Query(clauses=[clause])
    except ValueError as err:
        raise BuildQueryError(err)
    if _log.is_file:
        qobj = search.LogFile(_log.path)
        for rec in qobj.query(qry):

            if fields:
                rec = rec.project(fields, copy=True)
            yield rec
    else:
        raise NotImplementedError(
            'Only know how to search files, not {}'.format(_log.path))
