__author__ = 'jcorrea'

from tako.models.method_model import sinput
from tako.util import tigres

class Segmentation:

    def __init__(self, *args, **kwargs):
        self.algorithm = {}
        self.data = {}
        self.params = {}
        self.output = {}

