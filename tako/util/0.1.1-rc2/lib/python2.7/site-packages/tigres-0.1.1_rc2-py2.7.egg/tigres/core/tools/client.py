import sys

from tigres.core.execution.utils import TaskClient
from tigres.core.execution.plugin.distribute import \
    ExecutionPluginDistributeProcess


def main():
    if len(sys.argv) < 4:
        print("Usage {} <host> <port> <server-key>".format(sys.argv[0]))
        exit()

    host = sys.argv[1]
    port = int(sys.argv[2])
    key = sys.argv[3]
    task_client = TaskClient(ExecutionPluginDistributeProcess.execute,
                             host=host, port=port,
                             secret_key=key)
    print("Tigres Client Started")
    task_client.run()


if __name__ == "__main__":
    main()

