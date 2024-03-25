#!/bin/bash

apikey=$1
umls_date=$2
# bashisms
# ${umls_date,,} example: 2022AA becomes 2022aa
# Download the umls file. Make sure to input your API-Key in download_from_umpls_api.sh
#
echo "Downloading umls-${umls_date} metathesaurus"
if ! [ -f umls-${umls_date}-full.zip ]
then
    bash download_from_umls_api.sh --apikey ${apikey} --link https://download.nlm.nih.gov/umls/kss/${umls_date}/umls-${umls_date}-full.zip
fi

#
# Unzip to the datadir
#
echo "Unzipping the metathesaurus file"
unzip -u umls-${umls_date}-full.zip -d ../../data/


#
# Look at the MD5 Sums
#
echo "Checking md5 checksum"
cd ../../data/${umls_date}-full/
md5sum -c "${umls_date}.MD5"


#
# The actual metathesaurs is contained in a zipped files
#
echo "unzip the meta thesaurasus files"

unzip -u "${umls_date,,}-1-meta.nlm"
unzip -u "${umls_date,,}-2-meta.nlm"


#
# Some of the metathesaurus files are split, so need to be catted back together
#
echo "Catting the metathesaurus files back together"

cd ${umls_date}/META
for FILE in MRCONSO MRHIER MRREL MRSAT MRXNW_ENG MRXW
    do
        if ! [ -f $(find . -name "$FILE.RRF") ]
        then
            echo "Constructing $FILE.RRF"
            # Cat the pieces together
            zcat $(find . -name "$FILE*.gz") | \
            tqdm --desc "Processing $FILE" --bytes \
            --total `du -cb $(find . -name "$FILE*.gz") | grep total | awk '{print $1}'` \
            > $FILE.RRF;
        else
            echo "$FILE.RRF already exists"
        fi
    done

echo "Completed downloading UMLS"