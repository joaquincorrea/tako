__author__ = 'jcorrea'

from tako.arms import reconstruction as RC
from tako.head.head import do_workflow

reconstruction = RC.Reconstruction

reconstruction.algorithm = {"method": "WBP",
                            "FOO": "BAR"}

reconstruction.data = "examples/data/data.raw"

reconstruction.params = {"FOO": "BAR",
                         }

do_workflow.inputs = {reconstruction}
do_workflow.inputs = reconstruction.params
