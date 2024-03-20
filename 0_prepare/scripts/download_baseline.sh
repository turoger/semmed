#!/bin/bash

# Make the baseline directory for download
mkdir -p ../../data/baseline
cd ../../data/baseline

# Download all the files
echo "Downloading baseline files"
wget -r -nv -N -c ftp://ftp.ncbi.nlm.nih.gov/pubmed/baseline/

# Fix the directory sturcture
echo "Fixing directory structure"
pushd ftp.ncbi.nlm.nih.gov/pubmed/baseline/
cp * ../../../
popd
#rm -rf ftp.ncbi.nlm.nih.gov

# Make sure all the files are OK
echo "Checking md5 checksum"
cat *.md5 > all.md5
md5sum -c all.md5

echo "Completed downloading baseline"