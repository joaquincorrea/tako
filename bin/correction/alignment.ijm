
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
run("Linear Stack Alignment with SIFT", "initial_gaussian_blur=1.60 steps_per_scale_octave=3 minimum_image_size=64 maximum_image_size=1024 feature_descriptor_size=4 feature_descriptor_orientation_bins=8 closest/next_closest_ratio=0.92 maximal_alignment_error=25 inlier_ratio=0.05 expected_transformation=Rigid interpolate");
// END ImageJ macro

saveAs("Tiff", output);
close();