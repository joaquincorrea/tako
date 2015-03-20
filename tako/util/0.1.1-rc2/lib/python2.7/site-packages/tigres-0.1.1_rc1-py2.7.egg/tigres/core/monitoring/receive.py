"""
`tigres.core.monitoring.receive`
================================

.. currentmodule:: tigres.core.monitoring.receive

:platform: Unix, Mac
:synopsis: Module for receiving records over a socket.

:Example:

>>> from tigres.core.monitoring import receive
>>> class DataPrinter(receive.Observer):
...     def update(self, data):
...         print("got data: {}".format(data))
>>> class StatusPrinter(receive.Observer):
...     def update(self, bytes):
...         print("so far, {:d} bytes".format(bytes))
>>> server = receive.TCPServer(54321)
>>> reporter = receive.Reporter(1024) # every 1K
>>> reporter.attach(StatusPrinter())
>>> server.attach(reporter, datatype=int)
>>> server.attach(DataPrinter(), datatype=str)
>>> server.close()


.. moduleauthor:: Dan Gunter <dkgunter@lbl.gov>

moduledate - 4/27/13

"""
import logging
import asyncore
import socket
import time

from tigres.core.monitoring import common




# # Logging
from tigres.core.utils import get_str

_logging = logging.getLogger('tigres.core.monitoring.receive')
_logging.setLevel(common.env_log_level(logging.INFO))
if not _logging.handlers:
    _logging.addHandler(logging.NullHandler())  # no whiners

## Constants

DEFAULT_PORT = 43123
DEFAULT_HOST = 'localhost'

## Server


class Subject(object):
    """
    Observer pattern that can filter on the
    type of the item, and also supports a clean shutdown.

    """

    def __init__(self):
        self._observers = {}

    def attach(self, observer, datatype=None):
        """
        Add an observer, with an optional specification of
        the type of object this observer will receive. If the
        datatype is left as None, then all items will be sent to the update method.

        :param observer: the observer to add
        :type observer: Observer
        :param datatype: the data type to look for

        """
        self._observers[observer] = datatype

    def detach(self, observer):
        """
        Removes the named observer

        :param observer: the observer to remove
        :type observer: Observer
        """
        if observer in self._observers:
            del self._observers[observer]

    def notify(self, item):
        """
        Notify the observers

        :param item:
        :return:
        """
        for observer, datatype in self._observers.items():
            if datatype is None or isinstance(item, datatype):
                observer.update(item)

    def shutdown(self):
        """Clean shutdown, by calling update_close() on all the observers.
        """
        obs, self._observers = self._observers.copy(), {}
        for observer in obs:
            observer.update_close()


class Observer(object):
    """Observer for the Observer pattern.
    """

    def update(self, item):
        pass

    def update_close(self):
        pass


class Report(object):
    def __init__(self):
        self.total_bytes = 0
        self.total_time = 0
        self.cur_bytes = 0
        self.cur_time = 0


class Reporter(Subject, Observer):
    """Report to observers every `n` items.
    """

    def __init__(self, n=65536):
        Subject.__init__(self)
        Observer.__init__(self)
        self._interval = n
        self._n = 0
        self._t = time.time()
        self._report = Report()

    def update(self, num):
        self._n += num
        if self._n >= self._interval:
            self._update_report()
            self._notify()

    def _update_report(self):
        r = self._report  # alias
        r.cur_time = time.time() - self._t
        r.cur_bytes = self._n
        r.total_time += r.cur_time
        r.total_bytes += r.cur_bytes
        self._t, self._n = time.time(), 0

    def _notify(self):
        self.notify(self._report)

    def update_close(self):
        """Do final report
        """
        if self._n > 0:
            self._update_report()
        self._notify()


class TCPServer(asyncore.dispatcher, Subject, Observer):
    """TCP socket server
    """

    def __init__(self, port=DEFAULT_PORT):
        asyncore.dispatcher.__init__(self)
        Subject.__init__(self)
        Observer.__init__(self)
        self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
        self.set_reuse_addr()
        self.bind(('', port))
        self.listen(5)
        self._stop = False

    def run(self):
        """Run until `close()` is called.
        """
        while not self._stop:
            try:
                asyncore.loop(timeout=1, count=100)
            except:
                break

    def handle_accept(self):
        conn, addr = self.accept()
        _logging.info(
            "accept.start host={host} port={port}".format(host=addr[0],
                                                          port=addr[1]))
        channel = InputChannel(conn, addr)
        channel.add_channel()
        channel.attach(self)  # get records from this channel in update()

    def handle_close(self):
        _logging.info("accept.end")
        self.shutdown()  # tell observers
        self.close()
        self._stop = True

    def update(self, item):
        """The received item can be either a record or an offset (report),
        from an InputChannel. The Subject superclass will send objects of the
        appropriate type to the appropriate observer(s).
        """
        self.notify(item)


class InputChannel(asyncore.dispatcher, Subject):
    """Read bytes, and send observer the text of each record.
    """
    RECORD_SEP = '\n'
    RECORD_SEP_LEN = len(RECORD_SEP)

    # Read block size
    block_size = 64 * 1024
    # Throughput report size
    report_size = 64 * 1024

    def __init__(self, sock, addr):
        asyncore.dispatcher.__init__(self, sock)
        Subject.__init__(self)
        self._buf = ""
        self._host, self._port = addr
        _logging.info(
            "conn.open host={host} port={port}".format(host=self._host,
                                                       port=self._port))

    def writable(self):
        return False

    def handle_close(self):
        _logging.info(
            "conn.close host={host} port={port}".format(host=self._host,
                                                        port=self._port))
        if self._buf:
            self._extract_records()
        self.close()

    def handle_read(self):
        """Read data from connection, break it into records,
        and invoke the call
        """
        data = self.recv(self.block_size)
        if len(data) == 0:
            return
        if not isinstance(data, str):
            data = get_str(data)
        self._buf += data
        offs = self._extract_records()
        # Shorten buffer to unsent portion
        if offs > 0:
            self._buf = self._buf[offs:]
        self.notify(offs)

    def _extract_records(self):
        """Extract records one at a time from the buffer,
        sending each to the observers, and return an offset after
        the last full record.
        """
        start, b = 0, self._buf
        while 1:
            end = b.find(self.RECORD_SEP, start, len(b))
            if end == -1:
                break
            next_start = end + self.RECORD_SEP_LEN
            self.notify(b[start:next_start])
            start = next_start
        return start


## Client

class ConnectionError(Exception):
    pass


class TCPClient(object):
    """Client to TCPServer.
    """

    def __init__(self, host=DEFAULT_HOST, port=DEFAULT_PORT, timeout=5):
        """Connect a new client for sending messages.

        :param host: Server host name or IP
        :type host: str
        :param port: Server port
        :type port: int
        :raise: ConnectionError on failure
        """
        try:
            self._conn = socket.create_connection((host, port),
                                                  timeout=timeout)
        except socket.error as err:
            raise ConnectionError(err)

    def send(self, data):
        """Send data to server.
        """
        self._conn.send(data.encode('utf-8'))

    def close(self):
        self._conn.close()

