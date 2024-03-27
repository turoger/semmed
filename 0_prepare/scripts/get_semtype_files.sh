#!/bin/bash

# Download the information on Semmantic Types in UMLS
# wget https://metamap.nlm.nih.gov/Docs/SemGroups_2013.txt -O ../../data/SemGroups_2013.txt
# wget https://metamap.nlm.nih.gov/Docs/SemanticTypes_2013AA.txt -O ../../data/SemanticTypes_2013AA.txt

# wget -c https://metamap.nlm.nih.gov/Docs/SemGroups_2018.txt -O ../../data/SemGroups_2018.txt
# wget -c https://metamap.nlm.nih.gov/Docs/SemanticTypes_2018AB.txt -O ../../data/SemanticTypes_2018AB.txt 
echo "Running get_semtype_files.sh"
echo "... downloading miscellaneous semtype files"
wget -c -N https://lhncbc.nlm.nih.gov/ii/tools/MetaMap/Docs/SemGroups_2018.txt -O ../../data/SemGroups_2018.txt
wget -c -N https://lhncbc.nlm.nih.gov/ii/tools/MetaMap/Docs/SemanticTypes_2018AB.txt -O ../../data/SemanticTypes_2018AB.txt 

echo "Completed downloading SemType files"