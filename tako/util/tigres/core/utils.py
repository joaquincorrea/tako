"""
.. currentmodule:: tigres.core.utils

:platform: Unix, Mac
:synopsis: The internal task api

`tigres.core.utils`
*******************

Core utilities for Tigres


Classes
=========
 * :py:class:`SingletonMeta` -  Singleton Metaclass for making a singleton class
 * :py:class:`TigresInternalException` - Is this used?


.. moduleauthor:: Val Hendrix <vhendrix@lbl.gov>

"""
import socket
import sys


def get_metaclass(cls, name):
    return cls(name, (object, ), {})


def get_str(s):
    if sys.version_info >= (3, 0, 0):
        # for Python 3
        if isinstance(s, bytes):
            s = s.decode('ascii')  # or  s = str(s)[2:-1]
    else:
        # for Python 2
        if isinstance(s, unicode):
            s = str(s)
    return s


def get_free_port():
    """
    Get a free port
    :return:
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(('localhost', 0))
    _, port = s.getsockname()
    s.close()
    return port


# noinspection PyAttributeOutsideInit,PyArgumentList
class SingletonMeta(type):
    """ Singleton Metaclass for making a singleton class """

    def __call__(cls, *args, **kwargs):
        """

        :param cls: Class object that is parent of this meta class
        :param args: positional arguments
        :param kwargs: keyword arguments
        :return: The singular instance
        """

        if '_instance' not in cls.__dict__ or cls._instance is None:
            cls._instance = super(SingletonMeta, cls).__call__(*args, **kwargs)
        return cls._instance


class TigresInternalException(Exception):
    pass