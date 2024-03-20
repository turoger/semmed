#!/bin/bash

apikey=$1

#
# Download the large file (about 1hr) and place it in the ../../data/ directory
#


NEW_FILE='../../data/semmedVER43_R.csv'
OUT_FILE='../../data/semmedVER43_2023_R_PREDICATION.csv.gz'

echo "downloading semmed files from UMLS to:"
echo $OUT_FILE

if ! [ -f $( find . -name semmedVER43_2023_R_PREDICATION.csv.gz* ) ]
then
    bash download_from_umls_api.sh --apikey $apikey --link https://data.lhncbc.nlm.nih.gov/umls-restricted/ii/tools/SemRep_SemMedDB_SKR/semmedVER43_2023_R_PREDICATION.csv.gz
fi

echo "copying semmed to $OUT_FILE"
cp semmedVER43_2023_R_PREDICATION.csv.gz* $OUT_FILE

#
# Column names already provided in directory
#
echo "Copying file names"
cp ../../data/col_names.txt $NEW_FILE


#
# Unzip files and combine them
#
echo Adding headers to ${NEW_FILE} and decompressing it

if ! [ -f $(find ../../data -name "semmed*.csv") ]
then
gzip -cd $OUT_FILE | (pv > $NEW_FILE)
#pv | zcat -cd $OUT_FILE | cat >> $NEW_FILE
else
echo "File already exists"
fi

echo "Completed downloading SemMedDB"