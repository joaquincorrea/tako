__author__ = 'jcorrea'

def f(x):
    return {
        'a': 1,
        'b': 2,
    }[x]

# setup = {"algorithm": "method1",
#                    "data": "examples/data/data.raw",
#                    "params": {"uno": 1,
#                               "dos": 2}}
#
# class Alignment:
#
#     def __init__(self, setup):
#         self.setup = setup
#         self.algorithm = self.methods(self.setup['algorithm'])
#             # self.setup['algorithm']
#
#     def methods(self, x):
#         # self.algorithm = ""
#         return {
#             'method1': "/Users/DOE6903584/NERSC/tako/bin/alignment/method",
#             'method2': None,
#             'methodN': None,
#     }[x]
#
#     def __main__(self):
#         pass
#         # self.algorithm = self.methods(self.setup['algorithm'])
#
#     #     self.name = "alignment"
#
#     def output(self):
#         pass
#
#     # var = methods()
#
# u = Alignment(setup)
# # u.setup = setup
# print(u.algorithm)

class Alignment:

    def __init__(self, *args, **kwargs):
        setup = kwargs.get('setup')
        self.setup = setup
        self.algorithm = setup['algorithm']
        self.method = self.methods()
        self.data = setup['data']
        self.params = setup['params']
            # self.setup['algorithm']

    def methods(self):
        return {
            'method1': "/Users/DOE6903584/NERSC/tako/bin/alignment/method",
            'method2': None,
            'methodN': None,
    }[self.algorithm]

    def __main__(self):
        pass
        # self.algorithm = self.methods(self.setup['algorithm'])

    #     self.name = "alignment"

    def output(self):
        pass

u = Alignment(setup={"algorithm": "method1",
                   "data": "examples/data/data.raw",
                   "params": {"uno": 1,
                              "dos": 2}})