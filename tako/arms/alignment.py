__author__ = 'jcorrea'

from tako.util import tigres

class Alignment:

    def __init__(self, *args, **kwargs):
        setup = kwargs.get("setup")
        self.setup = setup
        self.algorithm = setup['algorithm']
        self.method = self.methods()
        self.data = setup['data']
        self.params = setup['params']
        self.task = tigres.Task(self.algorithm, tigres.EXECUTABLE, self.method)
        self.sequence = tigres.sequence(self.algorithm, self.task, self.input_array())

    def methods(self):
        return {
            'method1': "/Users/DOE6903584/NERSC/tako/bin/alignment/method",
            'method2': None,
            'methodN': None,
    }[self.algorithm]

    def input_array(self):
        for item in self.params:
            pass

    def __main__(self):
        pass

    def output(self):
        pass
