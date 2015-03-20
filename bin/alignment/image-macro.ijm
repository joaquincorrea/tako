args = getArgument;
args = split(args,":")

//
//name = getArgument;
//if (name=="") exit ("No argument!");
//path = getDirectory("home")+"images"+File.separator+name;

img=args[0]
output=args[1]

setBatchMode(true);
open(img);
//print(getTitle+": "+getWidth+"x"+getHeight);

//open("/Users/DOE6903584/NERSC/tako/examples/data/Lenna.png");
run("Find Edges");
saveAs("Tiff", output);
close();

// /Applications/Fiji.app/Contents/MacOS/ImageJ-macosx --headless -macro /Users/DOE6903584/NERSC/tako/bin/alignment/image-macro.ijm /Users/DOE6903584/NERSC/tako/examples/data/Lenna.png:/Users/DOE6903584/NERSC/tako/examples/data/test.tiff -batch