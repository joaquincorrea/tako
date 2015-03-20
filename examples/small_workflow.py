__author__ = 'jcorrea'

from tako.arms.alignment import Alignment
from tako.arms.correction import Correction
from tako.arms.segmentation import Segmentation
from tako.arms.visualization import Visualization

import tako.arms
from tako.head import head

# Alignment block
alignment = Alignment(setup={"algorithm": "method1",
                   "data": "/Users/DOE6903584/NERSC/tako/examples/data/Lenna.png",
                   "params": ["--headless", "-macro", "/Users/DOE6903584/NERSC/tako/bin/alignment/image-macro.ijm"]})

head.do_workflow(setup={alignment})
