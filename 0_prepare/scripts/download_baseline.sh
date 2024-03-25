#!/bin/bash

# Make the baseline directory for download
mkdir -p ../../data/baseline
cd ../../data/baseline

# Download all the files
echo "Downloading baseline files"
wget -r -nv -N -c ftp://ftp.ncbi.nlm.nih.gov/pubmed/baseline/

# Fix the directory sturcture
if ! [ $(ls -al *.xml.gz | wc -l) -gt 1218 ];then
    echo "Fixing directory structure"
    pushd ftp.ncbi.nlm.nih.gov/pubmed/baseline/
    cp * ../../../
    popd
    # rm -rf ftp.ncbi.nlm.nih.gov
else
    echo "Directory structure is OK"
fi

# Make sure all the files are OK
echo "Checking md5 checksum"
 if [ -f $(find . -name all.md5) ]; then
    echo "MD5 checksum file exists"
else
    echo "MD5 checksum file does not exist. Catting all md5 files to all.md5"
    cat *.md5 > all.md5
    md5sum -c all.md5
fi

echo "Completed downloading baseline"