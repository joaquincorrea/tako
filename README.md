
タコ (Tako)
===
Tako is a image processing workflow engine for high-performance computing

What it does!
---
Tako reduces the complexity of high-performance image processing by abstracting the software-stack and the data-management layer, allowing complex workflows to happen with very little effort involved. This is possible by having 
a workflow engine and a common data format between stages, one stage's output can be the next stage's input.
 
*i.e.* A workflow that requires the sequence **image alignment>correction>segmentation>visualization** can be written 
in `tako` like this:

  - `examples/big_workflow.py`

```
#!python
    # Import tako.arms
    from tako.arms.alignment import Alignment
    from tako.arms.correction import Correction
    from tako.arms.segmentation import Segmentation
    from tako.arms.visualization import Visualization
    from tako.head import head
```

  - Define each stage with a dictionary `setup`
  
```
#!python
    # Alignment block
    alignment = Alignment(setup={"algorithm": "sift",
                   "data": "examples/data/data.raw",
                   "params": {"fnum": 10,
                              "sigma": 0.001}})
```

```
#!python
    # Correction block
    correction = Correction(setup = {setup={"algorithm": "drift-correction",
                       "data": alignment.output(),
                       "params": {"elastic": -0.2})
```

```
#!python
    # Segmentation block
    segmentation = Segmentation(setup = {"algorithm": "mean-shift",
                       "data": correction.output(),
                       "params": {"sigma": 0.1,
                                  "epsilon": 0.01}})
```

```
#!python
    # Visualization block
    visualization = Visualization(setup = {"algorithm": "3d-render",
                       "data": segmentation.output(),
                       "params": {"res": 100})
```

```
#!python
    # Execution block
    head.do_workflow.inputs(inputs=[alignment, correction, segmentation, visualization])
```

Tako and ImageJ
---
Tako is also capable of running ImageJ macros or plugins as a batch job

*i.e.*
```
#!python
    # examples/imagej_macro.py
    from tako.arms.correction import Correction
    from tako.head import head
    
    alignment = Correction(setup={"algorithm": "ijmacro",
                       "data": "/Users/DOE6903584/NERSC/tako/examples/data/Lenna.png",
                       "params": {'macro': "/Users/DOE6903584/NERSC/tako/bin/correction/image-macro.ijm"}
                       }
                          )
    
    do = head.do_workflow(setup=[alignment])
```

```
#!java
    // bin/correction/image-macro.ijm
    args = getArgument;
    args = split(args,":")
    
    img=args[0]
    output=args[1]
    
    setBatchMode(true);
    open(img);
    
    run("Find Edges");
    saveAs("Tiff", output);
    close();
```

Architecture
---
タコ (Tako) has two main components, `tako.arms` where the workflows are templated, and `tako.head` where the workflow engine [[Tigres]] manages arm assignments.

Credits
---

  - Joaquin Correa
  - Tigres team
