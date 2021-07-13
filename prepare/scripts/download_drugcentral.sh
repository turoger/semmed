#!/bin/bash

# Fill in the following for location of your postgreSQL server
DUMPDATE=20200918
HOST=
PORT=
USER=
DUMPDIR=$(pwd | xargs dirname)'/data/drugcentral_'
#
# Download the Drug Central Dump
#
#echo "Downloading Drug Central Dump"
wget http://unmtid-shinyapps.net/download/drugcentral-pgdump_$DUMPDATE.sql.gz -O ../data/drugcentral.dump.$DUMPDATE.sql.gz


#
# The remainer of this file requires postgreSQL to be installed and running on the machine
#
echo "Creating postgreSQL database at ${HOST}:${PORT} as user: ${USER}"
createdb drugcentral_$DUMPDATE -h $HOST -p $PORT -U $USER

echo "Unzipping drugcentral dataset dump ${DUMPDATE}"
gunzip < ../../data/drugcentral.dump.$DUMPDATE.sql.gz | psql drugcentral_$DUMPDATE -h $HOST -p $PORT -U $USER

#
# Copy the required tables to disk for easy reading
#
echo "Copying 'omop_relations' to 'rel'"
psql -U $USER -h $HOST -p $PORT -d drugcentral_$DUMPDATE -c "\COPY omop_relationship TO $DUMPDIR'rel_${DUMPDATE}.csv DELIMITER ',' CSV HEADER"

echo "Copying 'identifier' to 'ids'"
psql -U $USER -h $HOST -p $PORT -d drugcentral_$DUMPDATE -c "\COPY identifier TO $DUMPDIR'ids_${DUMPDATE}.csv DELIMITER ',' CSV HEADER"

echo "Copying 'approval' to 'approvals'"
psql -U $USER -h $HOST -p $PORT -d drugcentral_$DUMPDATE -c "\COPY approval TO $DUMPDIR'approvals_${DUMPDATE}.csv DELIMITER ',' CSV HEADER"

echo "Copying 'synonyms' to 'syn'"
psql -U $USER -h $HOST -p $PORT -d drugcentral_$DUMPDATE -c "\COPY synonyms TO $DUMPDIR'syn_${DUMPDATE}.csv DELIMITER ',' CSV HEADER"

echo "Copying 'atc' to 'atc'"
psql -U $USER -h $HOST -p $PORT -d drugcentral_$DUMPDATE -c "\COPY atc TO $DUMPDIR'atc_${DUMPDATE}.csv DELIMITER ',' CSV HEADER"

echo "Copying 'atc_ddd' to 'atc-ddd'"
psql -U $USER -h $HOST -p $PORT -d drugcentral_$DUMPDATE -c "\COPY atc_ddd TO $DUMPDIR'atc-ddd_${DUMPDATE}.csv DELIMITER ',' CSV HEADER"

