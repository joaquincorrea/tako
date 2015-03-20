タコ (Tako)
===
Tako is a image processing workflow engine for high-performance computing

What it does!
---
Tako reduces the complexity of high-performance image processing by abstracting the software-stack and the data
management layer, allowing complex workflows to happen with very little effort involved. This is possible by having 
a workflow engine and a common data format between stages, one stage's output can be the next stage's input.
 
i.e. A workflow that requires the sequence image alignment->correction->segmentation->visualization can be written 
in `tako` like this:

```python
from tako.arms.alignment import Alignment
from tako.arms.correction import Correction
from tako.arms.segmentation import Segmentation
from tako.arms.visualization import Visualization
```
 
How to use
---
`examples/big_workflow.py`


Architecture
---
  - Head:
  - Arms:
  
Workflow engine
---
Tako currently uses Tigres, there are plans to support swift

Credits
---
