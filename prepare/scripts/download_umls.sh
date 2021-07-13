#!/bin/bash

apikey=$1

#
# Download the umls file. Make sure to input your API-Key in download_from_umpls_api.sh
#
echo "Downloading umls-2021AA metathesaurus"
bash download_from_umls_api.sh --api $apikey --link https://download.nlm.nih.gov/umls/kss/2021AA/umls-2021AA-full.zip


#
# Unzip to the datadir
#
echo "Unzipping the metathesaurus file"
unzip umls-2021AA-full.zip -d ../../data/


#
# Look at the MD5 Sums
#
echo "Checking md5 checksum"
cd ../../data/2021AA-full/
md5sum -c 2021AA.MD5


#
# The actual metathesaurs is contained in a zipped files
#
echo "unzip the meta thesaurasus files"

unzip 2021aa-1-meta.nlm
unzip 2021aa-2-meta.nlm


#
# Some of the metathesaurus files are split, so need to be catted back together
#
echo "Catting the metathesaurus files back together"

cd 2021AA/META
for FILE in MRCONSO MRHIER MRREL MRSAT MRXNW_ENG MRXW
do
    # Cat the pieces together
    zcat $(ls | grep $FILE) > $FILE.RRF
done
