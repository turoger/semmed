#!/bin/bash

apikey=$1
semmed_ver=$2
umls_date=$3

# bashisms
# ${semmed_ver%%_*} -> example: VER43_R becomes VER43
# ${umls_date//[!0-9]/} -> example: 2021AB becomes 2021
#
# Download the large file (about 1hr) and place it in the ../../data/ directory
#


NEW_FILE=../../data/semmed${semmed_ver}.csv
OUT_FILE=../../data/semmed${semmed_ver%%_*}_${umls_date//[!0-9]/}_R_PREDICATION.csv.gz
echo "Running download_semmeddb.sh"
echo "... downloading semmed files from UMLS to:"
echo $OUT_FILE

if ! [ -f $( find . -name semmed${semmed_ver%%_*}_${umls_date//[!0-9]/}_R_PREDICATION.csv.gz* ) ]
then
    echo "... Local copy of semmed not found. Downloading semmed from UMLS."
    bash download_from_umls_api.sh --apikey $apikey --link https://data.lhncbc.nlm.nih.gov/umls-restricted/ii/tools/SemRep_SemMedDB_SKR/semmed${semmed_ver%%_*}_${umls_date//[!0-9]/}_R_PREDICATION.csv.gz
else
    echo "... Local copy of semmed found."
fi
echo "... check if $OUT_FILE exists"
# Check if the file exists
if ! [ -f $OUTFILE ]; then
    echo "... Copying semmed to $OUT_FILE"
    cp semmed${semmed_ver%%_*}_${umls_date//[!0-9]/}_R_PREDICATION.csv.gz* $OUT_FILE
fi



#
# Unzip files and combine them
#
echo "... decompressing ${OUT_FILE} to ${NEW_FILE}"

if ! [ -f $(find ../../data -name "semmed*.csv") ]
then
    echo "... Copying file headers to $NEW_FILE"
    cp ../../data/col_names.txt $NEW_FILE
    echo "... Unzipping and adding to header"
    gzip -cd $OUT_FILE | pv >> $NEW_FILE
#pv | zcat -cd $OUT_FILE | cat >> $NEW_FILE
else
    echo "... File already exists"
fi

echo "Completed downloading SemMedDB"