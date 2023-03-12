 #!/bin/bash
 git clone https://github.com/Xtra-Computing/FedTree.git
 cd FedTree
 git submodule init
 git submodule update
 mkdir build
 cd build
 cmake ..
 make -j
 cd ../..
 cp FedTree/build/bin/FedTree-distributed-* src/unifed/framework/fedtree
