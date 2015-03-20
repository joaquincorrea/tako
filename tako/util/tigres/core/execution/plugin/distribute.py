"""
`tigres.core.execution.plugin.distribute`
========================

.. currentmodule:: tigres.core.execution.plugin.distribute

:platform: Unix, Mac
:synopsis: Execution Distributed plugin API

Classes
=======
 * :py:class:`ExecutionPluginDistributeProcess` - Execution plugin for distributing processes.



.. moduleauthor:: Val Hendrix <vchendrix@lbl.gov>

"""
import os
import socket
import subprocess
import uuid

from tigres.core.utils import get_free_port
from tigres.core.execution.plugin.local import ExecutionPluginLocalBase
from tigres.core.execution.utils import TaskServer, TaskClient


class ExecutionPluginDistributeProcess(ExecutionPluginLocalBase):
    """
    Execution plugin for distributing processes.
    """

    @classmethod
    def parallel(cls, parallel_work, run_fn):
        """ Executes in parallel across multiple hosts.
        Shared by parallel, split and merge templates

        Distributes tasks across the specified hosts machines. A
        `TaskServer` is run in the Tigres program.  `TigresClient`s which run workers
        that consume the tasks from the `TaskServer` queue are run on the
        specified hosts. If no host machines are specified the `TaskServer` and `TaskClients`
         will be run on the local machine.

        Environment variables:

          * `TIGRES_HOSTS` - a comma separated list of hosts names for distributing tasks.
                            (i. e. export TIGRES_HOSTS=host1,host2,host3)
          * `OTIGRES_*` - Any environment variable prefixed with `OTIGRES_` will pass to the
                          `tigres-client` program. (i.e export OTIGRES_PATH=tigres_home/bin)
                          will be translated to the command PATH=tigres_home/bin; tigres-client...)

        :param parallel_work: The parallel state object to execute.
        :type parallel_work: tigres.core.state.WorkParallel
        :param run_fn: function to state state with, it should take a WorkUnit as input
        """
        secret_key = str(uuid.uuid4())

        port = get_free_port()
        task_server = TaskServer(parallel_work, host=socket.gethostname(),
                                 port=port, secret_key=secret_key)

        # Launch Client(s)
        hosts = None
        if 'TIGRES_HOSTS' in os.environ:
            hosts = os.environ['TIGRES_HOSTS']

        if not hosts:
            def worker():
                task_client = TaskClient(cls.execute, host=socket.gethostname(),
                                     port=port, secret_key=secret_key)
                task_client.run()

            import multiprocessing
            task_client_process = multiprocessing.Process(target=worker)
            task_client_process.start()
            if not task_client_process:
                task_server.kill()
                raise Exception("There were no Tigres Clients Started")
            else:
                task_server.join()
                if task_client_process.is_alive():
                        task_client_process.terminate()
        else:
            programs = []
            host_list = hosts.split(',')
            env = " ".join(
                ["export {}={};".format(k.replace('OTIGRES_', ''),
                                        os.environ[k]) for k in os.environ if
                 k.startswith('OTIGRES_')])
            command = "bash --login -c '{} tigres-client {} {} {}'".format(env,
                                                                           socket.gethostname(),
                                                                           port,
                                                                           secret_key)
            for host in host_list:
                program=None
                try:
                    program = subprocess.Popen(
                        ["ssh", "-o", "StrictHostKeyChecking=no", host,
                         command], stderr=subprocess.PIPE, stdout=subprocess.PIPE)
                    if not program.returncode:
                        programs.append(program)
                    else:
                        if program.poll():
                            program.terminate()
                except Exception as err:
                    # TODO log this
                    program.terminate()
                    programs.remove(program)
                    print(err)
            if not len(programs) > 0:
                task_server.kill()
                raise Exception("There were no Tigres Clients Started")
            else:
                task_server.join()
                for p in programs:
                    if p.poll():
                        p.terminate()



