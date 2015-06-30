__author__ = 'jcorrea'

from tako.util import tigres
# from tako.arms.alignment import Alignment
import ipdb

class do_workflow:

    def __init__(self, *args, **kwargs):
        tasks = kwargs.get('setup')
        self.tasks = tasks

        # setup[0].task
        # setup[0].input
        self.Tasks = []
        self.Inputs = []

        for task in tasks:
            # self.Tasks.append(self.get_task(task))
            # self.Inputs.append(self.get_inputs(task))
            self.Tasks.append(task.task)
            self.Inputs.append(task.input)


        # ipdb.set_trace()
        tigres.sequence("", tigres.TaskArray("", self.Tasks), tigres.InputArray("",self.Inputs))
        # print(type(self.setup))


    def get_task(task):
        return task.task


    def get_inputs(task):
        return task.input



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