"""
`tigres.core.execution`
========================

.. currentmodule:: tigres.core.execution

:platform: Unix, Mac
:synopsis: The core tigres execution api


.. moduleauthor:: Val Hendrix <vchendrix@lbl.gov>

"""
from importlib import import_module
import sys

from tigres.core.execution.plugin import ExecutionPluginBase
from tigres.core.execution.plugin.distribute import \
    ExecutionPluginDistributeProcess
from tigres.utils import TigresException


__all__ = ['plugin', 'utils']


def load_plugin(execution):
    """
    Loads the execution plugin to tigres.core.execution namespace.

    :param execution: plugin to load
    :return: ExecutionPluginBase
    """
    if not execution:
        raise TigresException(
            "{}.{} - there is not execution to load".format(__name__,
                                                            load_plugin.__name__))
        # import execution plugin
    execution_split = execution.split('.')
    module_name = ".".join(execution_split[0:-1])
    module = import_module(module_name)
    plugin_module = getattr(module, execution_split[-1])
    if not issubclass(plugin_module, ExecutionPluginBase):
        raise TigresException(
            "{} is not a valid plugin. Must inherit from {}".format(execution,
                                                                    ExecutionPluginBase))
    sys.modules['tigres.core.execution.engine'] = plugin_module(execution)
    sys.modules[__name__].engine = sys.modules['tigres.core.execution.engine']
    return plugin_module

