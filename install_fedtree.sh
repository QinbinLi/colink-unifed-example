 #!/bin/bash
 git clone https://github.com/Xtra-Computing/FedTree.git
 cd FedTree
 git submodule init
 git submodule update
 mkdir build
 cd build
 cmake .. -DNTL_PATH="/home/qinbin/ntl/"
 make -j
 cd ../..
