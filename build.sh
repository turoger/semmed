#!/bin/bash

SCRIPT='build.sh'
#
# Help Message
#
Help()
{
	local message='$1'
	local txt=(
	"Description: Downloads SemMedDB, UMLS, and Drug Central to create a time-resolved drug repositioning framework."
    "	* SemMedDB version: 43"
    "	* UMLS version: "
    "	* Drug Central dump: 2021 Oct 05"
    ""
	"Usage: $SCRIPT [options] <command> [args]"
	""
	"Sample Commands"
	"bash $SCRIPT -H yourServerName -p 5432 -u yourUserName -a your_UMLS_API_Key" 
	"bash $SCRIPT --host yourServerName --port 5432 --user yourUserName"
	""
	"Options:"
	"	--host, -H			postgreSQL host name"
	"	--port, -p			postgreSQL port number"
	"	--user, -u			postgreSQL user login"
	"	--apikey, -a			UMLS apikey"
	"	--help, -h			optional, usage information"
	"	--negative, -n			optional, data generated will keep negative relations with this flag, default without the flag will remove negative relations"
    ""
    "NOTE:"
    "This code base is in development and has no guarantees. Use at your own risk."
	)
	printf "%s\n" "${txt[@]}"
}

#
# assign vars with short or long form flags
#
TEMP=$(getopt \
    --options a:H:u:p:n::h:: \
    --long apikey:,host:,user:,port:,negative::,help::\
    --name 'build' -- "$@"
    )
    
if [ $? != 0 ] ; then echo "Terminating..." >&2 ; exit 1 ; fi

APIKEY=
PORT=
HOST=
USER=
KEEP_NEGATIVE_EDGES=0
eval set --"$TEMP"
while true; do
    case "$1" in
        -h| --help)
            Help; exit ;;
        -a| --apikey) 
            APIKEY="$2"; 
            echo "APIKEY:    $APIKEY";
            shift 2 ;;
        -H| --host) 
            HOST="$2";
            echo "HOST:      $HOST";
            shift 2 ;;
        -p| --port)
            PORT="$2";
            echo "PORT:      $PORT";
            shift 2 ;;
        -u| --user) 
            USER="$2";
            echo "USER:      $USER";
            shift 2 ;;
        -n| --negative)
            KEEP_NEGATIVE_EDGES=1;
            echo "NEGATIVE:  $KEEP_NEGATIVE_EDGES";
            shift 2;;
        -- ) shift; break ;; # allows you to  break loop once all options exhausted
        *) echo "ERROR: Invalid Option"; Help; exit 1;;
    esac
done

#
# Check for missing inputs
#
if [[ -z $APIKEY || -z $HOST || -z $USER || -z $PORT ]]; then
    echo "ERROR: Missing APIKEY or HOST or USER input"
    echo "Please provide:"
    if [[ -z $APIKEY ]];then echo "    APIKEY (-a)"; fi
    if [[ -z $HOST ]]; then echo "    HOST (-H)"; fi
    if [[ -z $PORT ]]; then echo "    PORT (-p)"; fi
    if [[ -z $USER ]]; then echo "    USER (-u)"; fi
    echo "See Help (-h) for more information"
    exit 1
fi

# Get UMLS
cd ./0_prepare/scripts
# bash download_umls.sh $apikey

# Get Semmed
bash download_semmeddb.sh $apikey

# Get Semmed doc types
bash get_semtype_files.sh 

# Get Drug Central
bash download_drugcentral.sh --host $HOST --port $PORT --user $USER

# Get baseline
bash download_baseline.sh

cd ../../1_build

# process hetnet
echo "SEMMED Building HetNet"
if [[ $KEEP_NEGATIVE_EDGES -eq 1 ]]; then
    echo "Replacing Negative Relations with Bidirectional Relations"
python ./scripts/01_build_hetnet_and_reverse_neg_relation.py
else
    echo "Dropping Negative Relations"
python ./scripts/01_build_hetnet_and_drop_neg_relation.py
fi

# run rest of the pipeline
bash 00-execute_build.sh