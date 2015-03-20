"""
`tigres.core.monitoring.kvp`
============================

.. currentmodule:: tigres.core.monitoring.kvp

:platform: Unix, Mac
:synopsis: Processing of key/value pairs.


.. moduleauthor:: Dan Gunter <dkgunter@lbl.gov>

..code-block::

    from tigres.core.monitoring.kvp import Reader
    r = Reader('./a_file.log')
    for record in r:
       # each record is a dictionary
       ...
    # any iterable will also work as input
    r = Reader(['artist="Bob Dylan" album="Blood on the Tracks"',
            'artist="John Coltrane" album="Giant Steps"'])
    for info in r:
        print('{artist} made the album {album}'.format(**info))


"""
import importlib

try:
    range = importlib.import_module('xrange')
except ImportError:
    pass
from tigres.core.utils import get_str

ENCODING_UTF_8 = 'utf-8'
__date__ = '3/22/13'

import calendar
import codecs
from collections import OrderedDict
import json
import re
import time
from warnings import warn
# Package imports
from tigres.core.monitoring.common import Level, Keyword, MetaKeyword, \
    DEFAULT_ENCODING, META_LINE_MARKER, LOG_FORMAT_JSON, LOG_FORMAT_NL

# Regular expression to parse a text log record.
# Parameters are enclosed in {braces}, they will
# be filled in by format() before compiling the expression.
KVP_EXPR = r"""(?:
    \s*                        # leading whitespace
    ([0-9a-zA-Z_.\-]+)         # Name
    {sep}                      # Key/Value separator
    (?:                        # Value:
      ([^"\s]+) |              # a. simple value
      "((?:[^"] | (?<=\\)")*)" # b. quoted string
    )
    \s*
)"""


def parse_timestamp(ts):
    """Parse a timestamp

    :param ts: Timestamp in format YYYY-MM-DD:HH:MM:SS[.0123456...]
    :type ts: str
    :return: Seconds since the 1970-1-1 epoch
    :rtype: float
    """
    ts, subs = ts.split('.')
    if subs:
        subsec = float('.' + subs[:-1])
    else:
        subsec = 0
    return calendar.timegm(time.strptime(ts, r'%Y-%m-%dT%H:%M:%S')) + subsec


class Record(object):
    """Record used for formatting
    """
    # Fields that will always be present
    AUTO_FIELDS = {Keyword.TIME, Keyword.LEVEL}

    def __init__(self, rec=None):
        """Create new record.

        :param rec: Key/value pairs to create record from. NOTE: This input may be
                    modified in-place; pass a copy if you want to retain exact original.
        :type rec: dict
        """
        self._fields = OrderedDict()
        self._fields[Keyword.TIME] = 0
        self._fields[Keyword.LEVEL] = 0
        self._str_time = False
        self._time_string = None
        if rec:
            self.from_dict(rec)

    def project(self, fields, copy=True):
        """Project the record onto just these fields.

        :param fields: The fields to project onto
        :type fields: list of object
        :param copy: Copy to a new record, or in-place
        :type copy: bool
        :return: New record, or nothing if copy=False
        :rtype: Record or None
        """
        if copy:
            d = {k: self._fields[k] for k in fields if k in self._fields}
            d[Keyword.TIME] = self.ts
            d[Keyword.LEVEL] = self.level
            return Record(d)
        else:
            delete_me = [f for f in self._fields if
                         f not in self.AUTO_FIELDS and f not in fields]
            for field in delete_me:
                del self._fields[field]

    def as_dict(self):
        return self._fields

    def from_dict(self, rec):
        """Add values from dict

        Convert timestamp to a number of seconds since the epoch (1-1-1970)
        Convert the level name to a number.
        """
        if Keyword.TIME in rec:
            ts = rec[Keyword.TIME]
            if isinstance(ts, str):
                self._time_string = ts
                # defer parsing of time until it is accessed
                self._str_time = True
            else:
                self._time_string = None
                self._str_time = False
        else:
            self._time_string = '1970-01-01T00:00:00'
            self._fields[Keyword.TIME] = 0
            self._str_time = False
        if Keyword.LEVEL in rec:
            lvl = rec[Keyword.LEVEL]
            if isinstance(lvl, str):
                rec[Keyword.LEVEL] = Level.to_number(lvl)
        self._fields.update(rec)

    def intersect_equal(self, other, ignore=()):
        """Check whether all keys/values in given record, which are present in this
        one, are the same.

        :param other: The record to compare to
        :type other: Record
        :param ignore: Ignore these keys
        :type ignore: list of str
        :rtype: bool
        """
        for k, v in other._fields.items():
            if k in ignore:
                continue
            if k in self._fields and v != self._fields[k]:
                # print("@@ mismatch on {}: other {} != self {}".format(k, v, self._fields[k]))
                return False
        return True

    def _parse_time(self):
        """Deferred time parsing
        """
        ts = parse_timestamp(self._time_string)
        # print("@@ parse time: {}  {:f}".format(self.time_string, ts))
        self._fields[Keyword.TIME] = ts
        self._str_time = False

    # special properties

    @property
    def time_string(self):
        return self._time_string

    @property
    def ts(self):
        if self._str_time:
            self._parse_time()
        return self._fields[Keyword.TIME]

    @property
    def level(self):
        return self._fields['level']

    @property
    def event(self):
        return self._fields[Keyword.EVENT]

    @property
    def message(self):
        return self._fields[Keyword.MESSAGE]

    @property
    def status(self):
        return int(self._fields[Keyword.STATUS])

    def __getattr__(self, key):
        """Allow access to Tigres special fields by attribute.

        :return: Attribute value. Missing values return None.
        """
        if key.startswith(Keyword.pfx):
            return self._fields.get(key, None)
        else:
            return self.__dict__[key]

    # more special behavior

    def __iter__(self):
        return iter(list(self._fields.items())[2:])

    def __getitem__(self, item):
        if self._str_time and item == 'ts':
            self._parse_time()
        return self._fields[item]

    def __setitem__(self, item, value):
        self._fields[item] = value

    def __contains__(self, item):
        return item in self._fields

    def iteritems(self):
        return iter(self._fields.items())

    def get(self, item, default=None):
        return self._fields.get(item, default)

    def to_json(self):
        return dict(self._fields)

    def __str__(self):
        d = self.to_json()
        return json.dumps(d)


class Reader:
    ignore_bad = False

    def __init__(self, path_or_file, kv_sep='=', encoding=DEFAULT_ENCODING):
        """Create new reader with input object.

        :param path_or_file: Input
        :type path_or_file: str or iterable (ie has `next()`)
        :param kv_sep: Key/value separator char
        :type kv_sep: str
        :raises: ValueError if input is not a path, or not iterable
        """
        if isinstance(path_or_file, str):
            try:
                self._encoding = encoding.lower()
                self._in = codecs.open(path_or_file, mode='rb',
                                       encoding=self._encoding)
            except Exception as err:
                raise ValueError(
                    'Cannot open input file "{}": {}'.format(path_or_file, err))
        else:
            if not hasattr(path_or_file, 'readline'):
                raise ValueError(
                    'Input object, type {}, is not iterable'.format(
                        type(path_or_file)))
            self._in = path_or_file
            self._encoding = None
        self._expr = re.compile(KVP_EXPR.format(sep=kv_sep), flags=re.X)
        self._is_json = True

    def __iter__(self):
        return self

    def next(self):
        return self.__next__()

    def __next__(self):
        """Parse and return next record from input stream.

        :return: Record. If the record cannot be parsed and `ignore_bad` is
                 True, then an empty Record will be returned.
        :raises: ValueError on bad record, unless the class attribute `ignore_bad` is True.
                 StopIteration at end of data
        """
        # process initial metadata
        text = None
        while 1:
            try:
                text = next(self._in)
            except TypeError:
                text = self._in.next()
            if not isinstance(text, str):
                text = get_str(text)
            if not text.startswith(META_LINE_MARKER):
                break
            self._process_meta(text[len(META_LINE_MARKER):])
        if self._is_json:
            result = json.loads(text)
        else:
            result = self._parse_kvp(text)
        if not result and not self.ignore_bad:
            raise ValueError(
                "No key/value pairs in record: '{}'".format(text.strip()))
        return Record(result)

    def _parse_kvp(self, text):
        result = {}
        for n, v, vq in self._expr.findall(text):
            if not n:
                if self.ignore_bad:
                    continue
                else:
                    raise ValueError(
                        "Bad key '{}' in record: '{}'".format(n, text.strip()))
            if vq:  # quoted value
                v = vq.replace('\\"', '"')
            result[n] = v
        return result

    def _process_meta(self, text):
        kvp = json.loads(text)
        for name, value in kvp.items():
            value = value.lower()
            if name == MetaKeyword.ENCODING:
                if self._encoding is not None and value != self._encoding:
                    # .. TODO:: Re-open with proper encoding. This is tricker than it seems,
                    # .. TODO:: due to oddness of tell(); we will need to track position, in *bytes*, by hand.
                    warn(
                        "Reading log '{}' with wrong encoding: using {}, metadata says {}"
                        .format(self._in.name, self._encoding, value))
            elif name == MetaKeyword.FORMAT:
                if value == LOG_FORMAT_NL:
                    self._is_json = False
                elif value == LOG_FORMAT_JSON:
                    self._is_json = True
                else:
                    raise ValueError(
                        "Invalid log format ({}) in metadata: {}".format(value,
                                                                         text))


class Writer(object):
    """Base class for writing records to an output stream.
    """

    def __init__(self, ostream):
        """Build new writer.

        :param ostream: Output stream
        :type ostream: Anything with a write(str) method
        """
        self._out = ostream

    def close(self):
        """Explicit close.
        """
        pass

    def put(self, rec):
        """Put record onto output.

        :param rec: Input record object
        :type rec: dict or Record
        :return: Number of bytes written
        :rtype: int
        """
        raise NotImplementedError()

    def format(self, rec):
        """Format record

        :param rec: Input record object
        :type rec: dict or Record
        :return: formatted record
        :rtype: str
        """
        raise NotImplementedError()


class KvpFormatter(object):
    def __init__(self, log_format):
        self._is_json = log_format == LOG_FORMAT_JSON

    def _kvp_str(self, kvp, strip=True):
        if self._is_json:
            if strip:
                s = json.dumps(kvp)[1:-1]
            else:
                s = json.dumps(kvp)
        else:
            fields = []
            for k, v in kvp.items():
                # Are there double quotes or spaces in the string?
                if isinstance(v, str) and (' ' in v or '"' in v):
                    if '"' in v:
                        # Escape any existing double quotes
                        v = v.replace('"', '\\"')
                    # quote values with spaces
                    f = '{}="{}"'.format(k, v)
                else:
                    f = '{}={}'.format(k, v)
                fields.append(f)
            s = ' '.join(fields)
        return s


class LogWriter(Writer, KvpFormatter):
    """Write in classic name=value 'log' format.
    """

    def __init__(self, ostream=None, localtime=True, fmt=LOG_FORMAT_NL):
        KvpFormatter.__init__(self, fmt)
        if localtime:
            self._ttuple = time.localtime
            self._tzname = time.strftime('%z')
        else:
            self._ttuple = time.gmtime
            self._tzname = 'Z'
        Writer.__init__(self, ostream)

    def put(self, rec):
        return self._out.write(self.format(rec, strip=False))

    def format(self, rec, **kw):
        if isinstance(rec, dict):
            rec = Record(rec)
        ts_str = time.strftime('%Y-%m-%dT%H:%M:%S',
                               self._ttuple(rec.ts)) + self._tzname
        kvp = rec.as_dict()
        kvp.update({'ts': ts_str, 'level': Level.to_name(rec.level)})
        s = self._kvp_str(kvp, **kw)
        return s + '\n'


class TableWriter(Writer):
    RESERVED = (Keyword.TIME, Keyword.LEVEL, Keyword.EVENT)

    def __init__(self, ostream, idlen=5, pagelen=66, buflen=500):
        super(TableWriter, self).__init__(ostream)
        self._stash = []
        self._pagelen, self._buflen = pagelen, buflen
        self._pagepos = 0
        self._clear_widths()
        self._ids, self._newids, self._idpre = set(), set(), idlen
        self._closed = False
        self._widths = None

    def put(self, rec):
        if len(self._stash) > self._buflen:
            self._shorten_ids()
            self._dump()
            # print("@@ dump idlen={:d}".format(len(self._ids)))
        self._add_to_stash(rec)

    def close(self):
        """Write output on close.
        """
        if not self._closed:  # make idempotent
            self._dump(True)
        self._closed = True

    def _add_to_stash(self, rec):
        """Add record to stash

        :type rec: Record
        """
        self._stash.append(rec)
        for key, val in rec.items():
            w0 = self._widths.get(key, len(key))
            w = max(w0, len(str(val)))
            self._widths[key] = w
            if Keyword.is_id(key):
                if val not in self._ids:
                    self._update_prefixlen(val)
                self._ids.add(val)
                self._newids.add(val)

    def _clear_widths(self):
        self._widths = {}
        for key in self.RESERVED:
            self._widths[key] = len(key)

    def _dump(self, final=False):
        n = self._pagepos + len(self._stash)
        while n > self._pagelen:
            self._print_page()
            n -= self._pagelen
            self._pagepos = 0
        if final:
            self._print_page(n)
        else:
            self._pagepos = n
            self._clear_widths()
            self._ids, self._newids = self._newids, set()

    def _shorten_ids(self):
        """Shorten id's to prefix."""
        for name, w in self._widths.items():
            if Keyword.is_id(name):
                self._widths[name] = max(len(name), self._idpre)

    def _print_page(self, n=None):
        """Print one page, remove rows from stash."""
        # header
        columns = self._header()
        # body
        if n is None:
            n = self._pagelen - self._pagepos
        for i in range(n):
            rec = self._stash[i]
            row = []
            for name, fmtstr in columns:
                if Keyword.is_id(name):
                    s = rec.get(name, '')[:self._idpre]
                else:
                    s = str(rec.get(name, ''))
                row.append(fmtstr.format(s))
            self._out.write(' '.join(row) + '\n')
            # shorten stash
        self._stash = self._stash[n:]

    def _header(self):
        """Get column widths and print header.
        """
        # get widths
        columns, widths = [], self._widths.copy()
        for name in self.RESERVED:
            w = widths[name]
            columns.append((name, '{{:{:d}s}}'.format(w)))
            del widths[name]
        names = list(widths.keys())
        names.sort()
        for name in names:
            w = widths[name]
            columns.append((name, '{{:{:d}s}}'.format(w)))
            # print header
        row = [fmtstr.format(name) for name, fmtstr in columns]
        self._out.write('\n' + ' '.join(row) + '\n')
        row = ['-' * self._widths[name] for name, fmtstr in columns]
        self._out.write('-'.join(row) + '\n')
        # done
        return columns

    def _update_prefixlen(self, val):
        n = self._idpre
        for item in self._ids:
            while item[:n] == val[:n]:
                n += 1
        self._idpre = n