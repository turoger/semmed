#!/bin/bash

# Fill in the following for location of your postgreSQL server
DUMPDATE=20200918
DUMPDIR=$(pwd | xargs dirname)'/data/drugcentral_'
SCRIPT='download_drugcentral.sh'

#
# Usage
#
Help(){
	local message='$1'
	local txt=(
	"Description: Downloads data from Drug Central and processes it through a postgreSQL server, locally or externally"
	"Usage: $SCRIPT [options] <command> [args]"
	""
	"Sample Commands"
	"bash $SCRIPT -H yourServerName -p 5432 -u yourUserName"
	"bash $SCRIPT --host yourServer --port 5432 --user yourUserName"
	""
	"Options:"
	"	--help, -h	usage information"
	"	--host, -H	host name"
	"	--port, -p	port number"
	"	--user, -u	user login"
	)
	printf "%s\n" "${txt[@]}"
}

#
# Check for missing options
#
if [ $# -eq 0 ];
then
	echo "Please check for missing inputs"
	echo ""
	Help
elif [ $# -gt 6 ];
then
	echo "Too many inputs: $# given"
	echo ""
	Help
fi


#
# turn inputs into an array for options
#
vars=($@)

for i in "${!vars[@]}"
do
	if [[ "${vars[i]}" = '--host' || "${vars[i]}" = '-H' ]]
	then
		echo "HOST: ${vars[i+1]}"
		export HOST=${vars[i+1]}
	elif [[ "${vars[i]}" = '--port' || "${vars[i]}" = '-p' ]]
	then
		echo "PORT: ${vars[i+1]}"
		export PORT=${vars[i+1]}
	elif [[ "${vars[i]}" = '--user' || "${vars[i]}" = '-u' ]]
	then
		echo "USER: ${vars[i+1]}"
		export USER=${vars[i+1]}
	elif [[ "${vars[i]}" = '--help' || "${vars[i]}" = '-h' ]]
	then
		Help
		exit 0
	fi
done








#
# Download the Drug Central Dump
#
echo "Downloading Drug Central Dump"
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

echo "Done downloading and processing download_drugcentral.sh"
