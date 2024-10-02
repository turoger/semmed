#!/bin/bash

# Download the information on Semmantic Types in UMLS
# Old file locations
# wget https://metamap.nlm.nih.gov/Docs/SemGroups_2013.txt -O ../../data/SemGroups_2013.txt
# wget https://metamap.nlm.nih.gov/Docs/SemanticTypes_2013AA.txt -O ../../data/SemanticTypes_2013AA.txt

# wget https://metamap.nlm.nih.gov/Docs/SemGroups_2018.txt -O ../../data/SemGroups_2018.txt
# wget https://metamap.nlm.nih.gov/Docs/SemanticTypes_2018AB.txt -O ../../data/SemanticTypes_2018AB.txt 

# wget https://lhncbc.nlm.nih.gov/ii/tools/MetaMap/Docs/SemGroups_2018.txt -O ../../data/SemGroups_2018.txt
# wget https://lhncbc.nlm.nih.gov/ii/tools/MetaMap/Docs/SemanticTypes_2018AB.txt -O ../../data/SemanticTypes_2018AB.txt 

echo "Running get_semtype_files.sh"
echo "... downloading miscellaneous semtype files"

if ! [ -f ../../data/SemGroups.txt ]
then
    echo "... Local copy of SemGroups.txt not found. Downloading SemGroups.txt."
    wget https://lhncbc.nlm.nih.gov/semanticnetwork/download/SemGroups.txt -O ../../data/SemGroups.txt
else
    echo "... Local copy of SemGroups.txt found. Skipping download."
fi

if ! [ -f ../../data/SemTypes.txt ]
then
    echo "... Local copy of SemTypes.txt not found. Downloading SemTypes.txt."
    wget https://lhncbc.nlm.nih.gov/semanticnetwork/download/sn_current.tgz
# uncompress the downloaded file
    echo "... Uncompressing the downloaded file"
    tar -xvzf sn_current.tgz
# create the SemTypes.txt file from free text files
    echo "... Creating SemTypes.txt from the downloaded files"
    python process_semtypes.py -i */SU -o ../../data/SemTypes.txt

else
    echo "... Local copy of SemTypes.txt found. Skipping download."
fi

echo "Completed downloading miscellaneous semtype files"