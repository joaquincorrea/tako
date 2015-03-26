
// imagej-macro.ijm
// Distributed as part of Tako
// Joaquin Correa
// Data and Analytics Services (DAS)
// NERSC 2015
// JoaquinCorrea@lbl.gov

// ImageJ from CLI
// $ /path/to/ImageJ --headless -macro /path/to/imagej-macro.ijm arg1:arg2:argN -batch
// (image in) img = arg1 | output = arg2 (image out)

// Get macro parameters from cli
args = getArgument;
args = split(args,":");
// Extend you model to use N parameters,
// i.e. paramN = args[N-1]

img=args[0];
output=args[1];

setBatchMode(true);
open(img);

// START ImageJ macro
setAutoThreshold("Default dark");
//run("Threshold...");
setAutoThreshold("Default dark stack");
setOption("BlackBackground", false);
run("Convert to Mask", "method=Default background=Dark calculate");
// END ImageJ macro

saveAs("Tiff", output);
close();
