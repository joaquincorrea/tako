__author__ = 'jcorrea'

import sys
sys.path.append("/Users/DOE6903584/NERSC/tako")
sys.path.append("/Users/DOE6903584/NERSC/tako/tako/util")

from tako.arms.correction import Correction
from tako.head import head


alignment = Correction(setup={"algorithm": "ijmacro",
                   "data": "/Users/DOE6903584/NERSC/tako/examples/data/myxo-small.tif",
                   "output":"/Users/DOE6903584/NERSC/tako/examples/demo/myxo-small_align.tif",
                   "params": {'macro': "/Users/DOE6903584/NERSC/tako/bin/correction/alignment.ijm"}
                   }
                      )


segmentation = Correction(setup={"algorithm": "ijmacro",
                   "data": alignment.output,
                   "output":"/Users/DOE6903584/NERSC/tako/examples/demo/myxo-small_seg.tif",
                   "params": {'macro': "/Users/DOE6903584/NERSC/tako/bin/segmentation/segmentation.ijm"}
                   }
                      )

visualization = Correction(setup={"algorithm": "ijmacro",
                   "data": segmentation.output,
                   "output":"/Users/DOE6903584/NERSC/tako/examples/demo/myxo-small_vis.tif",
                   "params": {'macro': "/Users/DOE6903584/NERSC/tako/bin/visualization/visualization.ijm"}
                   }
                      )

do = head.do_workflow(setup=[alignment, [segmentation]])
