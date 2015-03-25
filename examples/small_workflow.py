__author__ = 'jcorrea'

import sys
sys.path.append("/Users/DOE6903584/NERSC/tako/")
sys.path.append("/Users/DOE6903584/NERSC/tako/tako/util")

from tako.arms.alignment import Alignment
from tako.arms.correction import Correction
from tako.arms.segmentation import Segmentation
from tako.arms.visualization import Visualization

import tako.arms
from tako.head import head

# Alignment block
alignment = Alignment(setup={"algorithm": "method1",
                   "data": "/Users/DOE6903584/NERSC/tako/examples/data/Lenna.png",
                   "params": {'macro': "/Users/DOE6903584/NERSC/tako/bin/alignment/imagej-macro.ijm"}
                   }
                      )

do = head.do_workflow(setup=[alignment])
