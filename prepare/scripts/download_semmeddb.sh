#!/bin/bash

apikey=$1

#
# Download the large file (about 1hr) and place it in the ../../data/ directory
#


NEW_FILE='../../data/semmedVER43_R.csv'
OUT_FILE='../../data/semmedVER43_2021_R_PREDICATION.sql.gz'

echo "downloading semmed files from UMLS to:"
echo $OUT_FILE

bash download_from_umls_api.sh --apikey $apikey --link https://data.lhncbc.nlm.nih.gov/umls-restricted/ii/tools/SemRep_SemMedDB_SKR/semmedVER43_2021_R_PREDICATION.sql.gz

mv semmedVER43_2021_R_PREDICATION.sql.gz* $OUT_FILE

#
# Column names already provided in directory
#
echo "Copying file names"
cp ../../data/col_names.txt $NEW_FILE


#
# pv allows progress to be monitored, if pv is not installed call the following line instead:
#
echo "Processing sql.gz"
#zcat ../data/semmedVER43_2021_R_PREDICATION.sql.gz | python ../mysqldump_to_csv.py >> $NEW_FILE
pv $OUT_FILE | zcat | python ../mysqldump_to_csv.py >> $NEW_FILE
