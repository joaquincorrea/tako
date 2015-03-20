"""
.. currentmodule:: tigres.core.execution.utils

:platform: Unix, Mac
:synopsis: Local (Desktop) Tigres Execution with qsub

`tigres.core.execution.utils`
*****************************

Batch queue execution for a Tigres Program


Functions
=========
 * :py:func:`create_executable_command` -  Create the executable command for the given task and input_values
 * :py:func:`run_command` -  Runs the command for the specified task
 * :py:func:`multiprocess_worker` -  Runs multiple worker processes and waits for them to finish.

Classes
=========
 * :py:class:`TaskProcessExecution` - Launches multiple processes to execute Tigres tasks in parallel
 * :py:class:`TaskThreadExecution` - Launches multiple threads to execute Tigres tasks in parallel
 * :py:class:`TaskServer` - Task Server for managing Tigres task execution across multiple hosts
 * :py:class:`TaskClient` - Task Client for executing Tigres tasks


.. moduleauthor:: Val Hendrix <vhendrix@lbl.gov>

"""
import time

try:
    import queue
except ImportError:
    import Queue as queue

try:
    xrange
    range = xrange
except NameError:
    pass

import multiprocessing
from multiprocessing.managers import SyncManager
import pickle
import subprocess
import threading
from tigres.core.utils import get_str

try:
    from cloud import cloud

    pickle = cloud.serialization.cloudpickle
except ImportError:
    import pickle

from tigres.utils import TigresException, TaskFailure, State


def create_executable_command(input_values, task):
    """

    :param task: the task for execution
    :type task: tigres.type.Task
    :param input_values: the input values for the task execution
    :type input_values: tigres.type.InputValues
    :return: a command to be run on the command line.
    :rtype: basestring


    """
    cmd = task.impl_name
    try:
        # Coerce the input values to strings
        input_values_str = [str(v) for v in input_values]

        if input_values:
            cmd = cmd + ' ' + ' '.join(input_values_str)
    except Exception as e:
        raise TigresException(
            "Exception caught for Task '{}' and command '{}'. Error message: {%}"
            .format(task.name, cmd, e))
    return cmd


def multiprocess_worker(job_queue, result_queue, num_processes, worker):
    """
    Launches the given worker a job and result queue for the specified number
    of processes. Waits until all of the workers are finished.

    :param job_queue: contains the jobs that the worker needs to process
    :param result_queue: empty queue for the worker's results
    :param num_processes: the number of processes to launch
    :param worker: the worker function to pass the the processes
    """
    procs = []
    for _ in range(num_processes):
        p = multiprocessing.Process(
            target=worker,
            args=(job_queue, result_queue))
        procs.append(p)
        p.start()

    for p in procs:
        p.join()


def run_command(command, env=None):
    """
    Runs the command for the specified task

    :param env:
    :param command: the command line execution
    :return: the command output
    :rtype: str
    """
    prog = subprocess.Popen(command, stderr=subprocess.PIPE,
                            stdout=subprocess.PIPE,
                            env=env, shell=True)

    stdout, stderr = prog.communicate()
    if len(stderr) > 0:
        raise TigresException("EXECUTABLE '{}' failed with error:{} output:{}"
                              .format(command, stderr, stdout))

    return get_str(stdout).rstrip('\n')


def worker(results_queue, input_queue, plugin):
    """
    A worker function
    """

    while 1:
        # get task
        w = input_queue.get()
        if w is None:
            input_queue.task_done()
            results_queue.put(None)
            return  # stop on sentinel value, None

        try:
            results_queue.put((w[0], (None, State.RUN)))
            results_queue.put((w[0], plugin.execute(*w)))

        except Exception as err:
            results_queue.put((w[0], (
                TaskFailure("Exception caught for execution", error=err),
                State.FAIL)))

        # We are done with this task, for now
        input_queue.task_done()


class TaskProcessExecution(object):
    """ Launches multiple processes to execute Tigres tasks in parallel"""

    def _submit_work(self, work):
        """
        Submit a list of state items to the  queue
        :param work:
        :type list:
        :return:
        """
        # add the state to the process queue
        for w in work:
            try:
                self._work[w.name] = w
                work_tuple = (
                    w.name, w.inputs.task, w.inputs.values, w.execution_data)

                # can it be pickled?
                pickle.dumps(work_tuple)
                self._queue.put(work_tuple)
            except Exception as e:
                # FIXME do something better here
                print(e)


    def __init__(self, plugin, work):
        """Construct and run.

        :param plugin: Execution plugin
        """
        super(TaskProcessExecution, self).__init__()
        self._queue = multiprocessing.JoinableQueue(0)
        self._num_processes = multiprocessing.cpu_count()
        len_work = len(work)
        if len_work < self._num_processes:
            self._num_processes = len_work
        self._num_processes_started = 0
        self._task_processes = []
        self._joined = False
        self._process_manager = multiprocessing.Manager()
        self._results = multiprocessing.JoinableQueue(0)
        self._work = {}

        # Start the processes
        for _ in range(self._num_processes):

            try:
                t = multiprocessing.Process(target=worker, args=(
                    self._results, self._queue, plugin))
                self._task_processes.append(t)
                t.start()
                self._num_processes_started += 1
            except Exception as err:
                print(err)
                if t.is_alive():
                    t.terminate()
                while t.is_alive():
                    time.sleep(0.1)

        if self._num_processes_started:
            self._submit_work(work)

    def _gather_results(self):
        """
        Gathers the results as they come and updates the dictionary of
        state items
        :return: None
        """
        # Gather the results as they come in
        num_processes_finished = 0
        while 1:
            # get results
            work_results = self._results.get()
            if work_results is None:
                num_processes_finished += 1
                self._results.task_done()
                if num_processes_finished == self._num_processes:
                    break  # All of the processes have finished
            else:
                self._work[work_results[0]].results = work_results[1][0]
                self._work[work_results[0]].state = work_results[1][1]
                self._results.task_done()

    def join(self):
        """Blocks until all items in the internal FIFO queue have been gotten and processed.
        """
        # This is not thread safe but that should be OK
        if not self._joined:
            self._joined = True
            # Add sentinel value to stop each thread
            for _ in range(self._num_processes_started):
                self._queue.put(None)
                # Block for threads to finish, storing exceptions

            self._gather_results()

        self._queue.join()


class TaskThreadExecution(object):
    """  Launches multiple threads to execute Tigres tasks in parallel
    """

    def __init__(self, run_fn, num_threads=2):
        """Construct and run.

        :param num_threads: number of threads to run
        :param run_fn: the run function to use
        """
        super(TaskThreadExecution, self).__init__()
        self._queue = queue.Queue(0)
        self._num_threads = num_threads
        self._task_threads = []

        def worker():

            while 1:
                # get task
                work = self._queue.get()
                if work is None:
                    self._queue.task_done()
                    break  # stop on sentinel value, None

                try:
                    run_fn(work)
                except TigresException as err:
                    work.results = TaskFailure(
                        "Task Execution Failure for {}".format(work.name),
                        error=err)
                    work.state = State.FAIL


                # We are done with this task, for now
                self._queue.task_done()

        for _ in range(num_threads):
            t = threading.Thread(target=worker)
            self._task_threads.append(t)
            t.start()

    def submit(self, work):
        """Submits the tasks to the internal FIFO queue for execution.
        """
        self._queue.put(work)

    def join(self):
        """Blocks until all items in the internal FIFO queue have been gotten and processed.
        """
        # Add sentinel value to stop each thread
        for _ in range(self._num_threads):
            self._queue.put(None)
            # Block for threads to finish, storing exceptions
        self._queue.join()


class TaskServerQueueManager(SyncManager):
    pass


class TaskServer(object):
    """ Task Server for managing Tigres task execution across multiple hosts """


    def __init__(self, work_list, host='localhost', port=0, secret_key=None):
        """

        :param host: task server host name
        :param port: task server port
        :param secret_key: the secret key shared by task and client
        :return:
        """
        job_queue = multiprocessing.Queue()
        result_queue = multiprocessing.Queue()
        self._address = (host, port)
        self._results = {}
        self._work = {}
        self._count_work = 0
        self._count_finished_work = 0

        TaskServerQueueManager.register('job_queue', callable=lambda: job_queue)
        TaskServerQueueManager.register('result_queue',
                                        callable=lambda: result_queue)
        self._manager = TaskServerQueueManager(address=self._address,
                                               authkey=secret_key.encode(
                                                   'utf-8'))

        self._manager.start()
        job_queue = self._manager.job_queue()
        for w in work_list:
            self._count_work += 1
            self._work[w.name] = w
            serialized_work = pickle.dumps(
                (w.name, w.inputs.task, w.inputs.values,
                 w.execution_data))
            job_queue.put(serialized_work)

    def join(self):
        job_queue = self._manager.job_queue()
        result_queue = self._manager.result_queue()

        # wait for the job_queue to be empty
        while self._count_finished_work < (self._count_work*2):
            while not result_queue.empty():
                self._count_finished_work += 1
                work_results = result_queue.get()
                self._work[work_results[0]].results = work_results[1][0]
                self._work[work_results[0]].state = work_results[1][1]

        self._manager.shutdown()

    def kill(self):
        self._manager.shutdown()


class TaskClientQueueManager(SyncManager):
    pass


class TaskClient(object):
    """
    Task Client for executing Tigres tasks
    """

    def worker(self, job_queue, result_queue):
        """

        :param job_queue:
        :param result_queue:
        :return:
        """
        while True:
            try:
                work = job_queue.get_nowait()
                work = pickle.loads(work)
                result_queue.put((work[0], (None, State.RUN)))
                result_queue.put((work[0], self._execute_fn(*work)))
            except queue.Empty:
                return
            except Exception as err:
                result_queue.put((work[0], (
                    TaskFailure("Task Execution Failure for {}".format(work[0]),
                                error=err),
                    State.FAIL)))

    def __init__(self, execute_fn, host='localhost', port=44555,
                 secret_key=None):
        """
        Task Client for executing Tigres tasks

        :param execute_fn:  The execute function
        :param host:
        :param port:
        :param secret_key:
        :return:
        """

        TaskClientQueueManager.register('job_queue')
        TaskClientQueueManager.register('result_queue')
        self._execute_fn = execute_fn

        self._manager = TaskClientQueueManager(address=(host, port),
                                               authkey=secret_key.encode(
                                                   'utf-8'))
        self._manager.connect()

    def run(self):
        job_queue = self._manager.job_queue()
        result_queue = self._manager.result_queue()

        multiprocess_worker(job_queue, result_queue,
                            multiprocessing.cpu_count(), self.worker, )




