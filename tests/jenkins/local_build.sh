#!/bin/sh
ROOTDIR=$(pwd)
DPKG_REFROOT=$ROOTDIR/dpkg/refroot/amd64
INSTALL_DIR=$DPKG_REFROOT/opt/bb

# install dependencies of nano
mkdir dpkg; cd dpkg; dpkg-distro-dev init
dpkg-refroot-install --distribution=unstable libcorokafka-dev -y
dpkg-refroot-install --distribution=unstable libcppkafka-dev -y
dpkg-refroot-install --distribution=unstable libquantum-dev -y
dpkg-refroot-install --distribution=unstable libgtest-dev -y
dpkg-refroot-install --distribution=unstable libgmock-dev -y
dpkg-refroot-install --distribution=unstable librhstsvcmsg-dev -y


# create makefiles
cd $ROOTDIR
bbcmake --version
bbcmake -64 \
	-DDPKG_REFROOT=$DPKG_REFROOT \
	-DBOOST_ROOT=$INSTALL_DIR \
	-DGTEST_ROOT=$INSTALL_DIR \
	-DRDKAFKA_ROOT=$INSTALL_DIR \
	-DQUANTUM_ROOT=$INSTALL_DIR \
	-DCppKafka_DIR=$INSTALL_DIR/lib64/cmake/CppKafka \
	-DQuantum_DIR=$INSTALL_DIR/lib64/cmake/Quantum \
	-DCOROKAFKA_ENABLE_TESTS=ON \
	-G "CodeBlocks - Unix Makefiles" -B build


# clean old files and rebuild tests
cmake --build build --target clean
cmake --build build --target all -j 11

exit 0
