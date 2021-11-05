#!/bin/bash

# enter your UMLS apikey 
apikey=

# enter your postgreSQL server information
HOST=
PORT=
USER=




Help()
{
        # Display Help
        echo "Description: Downloads all required files for this directory"
        echo "This script will download SemMedDB, UMLS and Drug Central"
        echo "Syntax: script Template [-h]"
        echo "options:"
        echo "--help, -h        prints help"
}

if [[ $1 = '-h' || $1 = '--help' ]];then
	Help
	exit 0
fi



# Get UMLS
cd ./scripts
bash download_umls.sh $apikey

# Get Semmed
bash download_semmeddb.sh $apikey

# Get Semmed doc types
bash get_semtype_files.sh 

# Get Drug Central
bash download_drugcentral.sh --host $HOST --port $PORT --user $USER

# Get baseline
bash download_baseline.sh
