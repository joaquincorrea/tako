__author__ = 'jcorrea'

from tako.util import tigres
# from tako.arms.alignment import Alignment
import ipdb

class do_workflow:

    def __init__(self, *args, **kwargs):
        setup = kwargs.get('setup')
        self.setup = setup

        Task = setup[0].task
        Input = setup[0].input
        # ipdb.set_trace()
        tigres.sequence("", tigres.TaskArray("", [Task]), tigres.InputArray("",[Input]))
        # print(type(self.setup))

        # self.tasks = self.task_gen()
        # ipdb.set_trace()

        # self.function_name = function_name
        # return inputs
        # self.inputs = tigres.InputArray("", [values])

    # def task_gen(self):

        # def get_function(self, task):
        #     function_name = task.__name__
        #     return "tako.arm.%s" % function_name
        # self.function_name = function_name

        # for stage in self.setup:
        #     pass

        # pass
        # for stage in self.inputs:
        #     stage.task
        #     stage.input

        # count = len(self.inputs)
        # taskArray = []
        # tasks = []
        # input_vals = []
        # for i, task in self.inputs:
        #     tasks[i] = tigres.Task(str(task), tigres.FUNCTION, self.get_function(task))
        #     input_vals[i] = tigres.InputValues(str(task), [])
        # taskArray = tigres.TaskArray("", tasks)
        # return tasks, input_vals

    # tigres.TaskArray("")
# tigres.InputValues()
#     tasks, input_vals = task_gen()
#     tigres.sequence("", tasks, input_vals)
# tigres.parallel()
# tigres.merge()
# tigres.split()