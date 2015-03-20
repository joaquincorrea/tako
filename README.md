<<<<<<< HEAD
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

Architecture
---
タコ (Tako) has two main components, `tako.arms` where the workflows are templated, and `tako.head` where the workflow engine [[Tigres]] manages arm assignments.

Credits
---

  - Joaquin Correa
  - Tigres team
=======
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

  - Define each stage with a dictiorary `setup`
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
    correction = Correction
    correction.setup = {setup={"algorithm": "drift-correction",
                       "data": alignment.output(),
                       "params": {"elastic": -0.2}
```

```
#!python
    # Segmentation block
    segmentation = Segmentation
    segmentation.setup = {"algorithm": "mean-shift",
                       "data": correction.output(),
                       "params": {"sigma": 0.1,
                                  "epsilon": 0.01}}
```

```
#!python
    # Visualization block
    visualization = Visualization
    visualization.setup = {"algorithm": "3d-render",
                       "data": segmentation.output(),
                       "params": {"res": 100}
```

```
#!python
    # Execution block
    head.do_workflow.inputs(inputs=[alignment, correction, segmentation, visualization])
```

Architecture
---
タコ (Tako) has two main portions, `tako.arms` where the workflows are templated `tako.head` where the workflow engine [[Tigres]] manages arm assignmetns.

Credits
---

  - Joaquin Correa
  - Tigres folks
>>>>>>> 2b4bf8992de545950515faecfa5ea227f519be37
