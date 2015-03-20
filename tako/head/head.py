__author__ = 'jcorrea'

from tako.util import tigres

class do_workflow(object):

    def __init__(self, *args, **kwargs):
        inputs = kwargs.get("inputs")
        self.inputs = inputs
        self.tasks = self.task_gen()
        # self.function_name = function_name
        # return inputs
        # self.inputs = tigres.InputArray("", [values])

    def task_gen(self):

        def get_function(self, task):
            function_name = task.__name__
            return "tako.arm.%s" % function_name
        # self.function_name = function_name

        # for stage in stages:
        #     for step in steps:
        count = len(self.inputs)
        taskArray = []
        tasks = []
        input_vals = []
        for i, task in self.inputs:
            tasks[i] = tigres.Task(str(task), tigres.FUNCTION, self.get_function(task))
            input_vals[i] = tigres.InputValues(str(task), [])
        taskArray = tigres.TaskArray("", tasks)
        return tasks, input_vals

    # tigres.TaskArray("")
# tigres.InputValues()
    tasks, input_vals = task_gen()
    tigres.sequence("", tasks, input_vals)
# tigres.parallel()
# tigres.merge()
# tigres.split()