__author__ = 'jcorrea'

from tako.arms.correction import Correction
from tako.head import head

correction = Correction(setup={"algorithm": "ijmacro",
                   "data": "/Users/DOE6903584/NERSC/tako/examples/data/Lenna.png",
                   "params": {'macro': "/Users/DOE6903584/NERSC/tako/bin/correction/imagej-macro.ijm"}
                   }
                      )

do = head.do_workflow(setup=[correction])
