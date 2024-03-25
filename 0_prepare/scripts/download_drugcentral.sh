#!/bin/bash

# Fill in the following for location of your postgreSQL server
DUMPDATE=20220822
DUMPDIR='../../data/drugcentral_'
#DUMPDIR=$(pwd | xargs dirname)'/data/drugcentral_'
SCRIPT='download_drugcentral.sh'

#
# Usage
#
Help(){
	local message='$1'
	local txt=(
	"Description: Downloads data from Drug Central and processes it through a postgreSQL server, locally or externally"
    ""
	"Usage: $SCRIPT [options] <command> [args]"
	""
	"Sample Commands"
	"bash $SCRIPT -H yourServerName -p 5432 -u yourUserName"
	"bash $SCRIPT --host yourServer --port 5432 --user yourUserName"
	""
	"Options:"
	"	--host, -H	host name"
    "   --pass, -P  password"
	"	--port, -p	port number"
	"	--user, -u	user login"
    "	--help, -h	optional, usage information"
	)
	printf "%s\n" "${txt[@]}"
}

#
# Check for missing options
#
if [ $# -eq 0 ];
then
	echo "ERROR: Please check for missing inputs"
	echo ""
	Help
elif [ $# -gt 8 ];
then
	echo "ERROR: Too many inputs: $# given"
	echo ""
	Help
fi

#
# Process flagging
#
TEMP=$(getopt \
    --options H:P:p:u:h:: \
    --long host:,pass:,port:,user:,help:: \
    --name 'download_drugcentral' -- "$@"
    )
    
PASS=
PORT=
HOST=
USER=
eval set --"$TEMP"
while true; do
    case "$1" in
        -h | --help)
            Help; exit;;
        -P | --pass)
            PASS="$2";
            # echo "PASS:  $PASS";
            shift 2 ;;
        -p | --port)
            PORT="$2";
            # echo "PORT:  $PORT";
            shift 2 ;;
        -u | --user)
            USER="$2"
            # echo "USER:  $USER";
            shift 2 ;;
        -H | --host)
            HOST="$2"
            # echo "HOST:  $HOST"
            shift 2;;
        -- ) shift; break ;;
        * ) echo "ERROR:  Invalid Option"; echo "" ; Help; exit 1 ;;
    esac
done

DUMPDIR='../../data/drugcentral_'
if [[ -f "${DUMPDIR}rel_${DUMPDATE}.csv" && "${DUMPDIR}ids_${DUMPDATE}.csv" && "${DUMPDIR}approvals_${DUMPDATE}.csv" && "${DUMPDIR}syn_${DUMPDATE}.csv" && "${DUMPDIR}atc_${DUMPDATE}.csv" && "${DUMPDIR}atc-ddd_${DUMPDATE}.csv" ]]; then
    echo "DrugCentral Files already exist and have been processed. Exiting $SCRIPT"
    exit 0
fi


#
# Download the Drug Central Dump
#
echo "Downloading Drug Central Dump"
wget -c -N "https://unmtid-shinyapps.net/download/drugcentral.dump.${DUMPDATE}.sql.gz" -O "../../data/drugcentral.dump.${DUMPDATE}.sql.gz"


#
# The remainder of this file requires postgreSQL to be installed and running on the machine
# Make sure your USER has appropriate permissions. Login to your postgres and elevate user 
# with `alter user $USER createdb;`
#
echo "Creating postgreSQL database at ${HOST}:${PORT} as user: ${USER}"
PGPASSWORD=$PASS createdb drugcentral_$DUMPDATE -h $HOST -p $PORT -U $USER

echo "Unzipping drugcentral dataset dump ${DUMPDATE}"
gunzip < ../../data/drugcentral.dump.$DUMPDATE.sql.gz | PGPASSWORD=$PASS psql drugcentral_$DUMPDATE -h $HOST -p $PORT -U $USER

cd ../../data
DUMPDIR=$(pwd)'/drugcentral_'

#
# Copy the required tables to disk for easy reading
#
echo "Copying 'omop_relations' to 'rel'"
PGPASSWORD=$PASS psql -q -U $USER -h $HOST -p $PORT -d drugcentral_$DUMPDATE -c "\COPY omop_relationship TO '${DUMPDIR}rel_${DUMPDATE}.csv' DELIMITER ',' CSV HEADER"

echo "Copying 'identifier' to 'ids'"
PGPASSWORD=$PASS psql -q -U $USER -h $HOST -p $PORT -d drugcentral_$DUMPDATE -c "\COPY identifier TO '${DUMPDIR}ids_${DUMPDATE}.csv' DELIMITER ',' CSV HEADER"

echo "Copying 'approval' to 'approvals'"
PGPASSWORD=$PASS psql -q -U $USER -h $HOST -p $PORT -d drugcentral_$DUMPDATE -c "\COPY approval TO '${DUMPDIR}approvals_${DUMPDATE}.csv' DELIMITER ',' CSV HEADER"

echo "Copying 'synonyms' to 'syn'"
PGPASSWORD=$PASS psql -q -U $USER -h $HOST -p $PORT -d drugcentral_$DUMPDATE -c "\COPY synonyms TO '${DUMPDIR}syn_${DUMPDATE}.csv' DELIMITER ',' CSV HEADER"

echo "Copying 'atc' to 'atc'"
PGPASSWORD=$PASS psql -q -U $USER -h $HOST -p $PORT -d drugcentral_$DUMPDATE -c "\COPY atc TO '${DUMPDIR}atc_${DUMPDATE}.csv' DELIMITER ',' CSV HEADER"

echo "Copying 'atc_ddd' to 'atc-ddd'"
PGPASSWORD=$PASS psql -q -U $USER -h $HOST -p $PORT -d drugcentral_$DUMPDATE -c "\COPY atc_ddd TO '${DUMPDIR}atc-ddd_${DUMPDATE}.csv' DELIMITER ',' CSV HEADER"

echo "Done downloading and processing download_drugcentral.sh"
