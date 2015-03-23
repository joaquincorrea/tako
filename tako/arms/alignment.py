__author__ = 'jcorrea'

from tako.util import tigres

class Alignment:

    def __init__(self, *args, **kwargs):
        setup = kwargs.get('setup')
        self.setup = setup
        self.algorithm = setup['algorithm']
        self.data = setup['data']
        self.output = "%s.tiff" % setup['data']
        self.method = self.methods()
        self.params = setup['params']
        self.task = tigres.Task(self.algorithm, tigres.EXECUTABLE, self.method['bin'])
        self.input = tigres.InputValues("", ["%s %s" % (' '.join(self.params), ''.join(self.method['args']))])
        # []
        # self.sequence = tigres.sequence(self.algorithm, self.task, self.input_array())

    def methods(self, *args, **kwargs):
        return {
            'method1': {'bin': "/Applications/Fiji.app/Contents/MacOS/ImageJ-macosx",
                        'args': "%s:%s -batch" % (self.data, self.output)},
            'method2': None,
            'methodN': None,
    }[self.algorithm]

    # def input_array(self):
    #     for item in self.params:
    #         pass
    #
    # def __main__(self):
    #     pass
    #
    # def output(self):
    #     pass
