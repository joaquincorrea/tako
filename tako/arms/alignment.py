__author__ = 'jcorrea'

from tako.util import tigres

class Alignment:

    def __init__(self, *args, **kwargs):
        setup = kwargs.get('setup')
        self.setup = setup
        self.algorithm = setup['algorithm']
        self.data = setup['data']
        self.output = "%s.tiff" % setup['data']
        self.params = setup['params']
        self.method = self.methods()
        self.task = tigres.Task(self.algorithm, tigres.EXECUTABLE, self.method['bin'])
        self.input = self.method['input']

    def methods(self, *args, **kwargs):
        return {
            'method1': {'bin': "/Applications/Fiji.app/Contents/MacOS/ImageJ-macosx",
                        'args': "%s:%s" % (self.data, self.output),
                        'input': tigres.InputValues("", ["--headless -macro %s %s:%s -batch" % (self.params['macro'], self.data, self.output)])},
            'method2': None,
            'methodN': None,
    }[self.algorithm]