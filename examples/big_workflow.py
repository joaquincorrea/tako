
__author__ = 'jcorrea'

from tako.arms.alignment import Alignment
from tako.arms.correction import Correction
from tako.arms.segmentation import Segmentation
from tako.arms.visualization import Visualization

import tako.arms
from tako.head import head

# Alignment block
alignment = Alignment(setup={"algorithm": "method1",
                   "data": "examples/data/data.raw",
                   "params": {"uno": 1,
                              "dos": 2}})

# Correction block
correction = Correction
correction.setup = {"algorithm": "",
                   "data": alignment.output(),
                   "params": {"uno": 1,
                              "dos": 2}}

# Segmentation block
segmentation = Segmentation
segmentation.setup = {"algorithm": "",
                   "data": correction.output(),
                   "params": {"uno": 1,
                              "dos": 2}}

# Visualization block
visualization = Visualization
visualization.setup = {"algorithm": "",
                   "data": segmentation.output(),
                   "params": {"uno": 1,
                              "dos": 2}}

head.do_workflow.inputs(inputs=[alignment, correction, segmentation, visualization])
