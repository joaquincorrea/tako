
タコ (Tako)
===
Tako is a image processing workflow engine for high-performance computing

Dev version
===
This branch is under active development and is not stable, current efforts are focused on changing the 
workflow engine from `Tigres` to `Airflow` and some improvements for `ImageJ2`.

What it does!
---
Tako reduces the complexity of high-performance image processing by abstracting the software-stack and the data-management layer, allowing complex workflows to happen with very little effort involved. This is possible by having 
a workflow engine and a common data format between stages, a stage output can be a stage input.
 
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
Tako also allows you to integrate ImageJ macros and plugins into your workflows.

*i.e.* Edge detection using ImageJ's `run("Find Edges");` method. 

`examples/imagej_macro.py`
```
#!python
    from tako.arms.correction import Correction
    from tako.head import head
    
    correction = Correction(setup={"algorithm": "ijmacro",
                       "data": "examples/data/Lenna.png",
                       "params": {'macro': "tako/bin/correction/imagej-macro.ijm"}
                       })
    
    do = head.do_workflow(setup=[correction])
```

`bin/correction/imagej-macro_template.ijm`
```
#!java
    args = getArgument;
    args = split(args,":");

    img=args[0];
    output=args[1];
    
    setBatchMode(true);
    open(img);
    
    // START ImageJ macro
    run("Find Edges");
    // END ImageJ macro
    
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
