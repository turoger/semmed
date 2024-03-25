#!/bin/bash

SCRIPT='download_requirements.sh'
#
# Help Message
#
Help()
{
	local message='$1'
	local txt=(
	"Description: Downloads datasets from SemMedDB, UMLS, and Drug Central to create a time-resolved drug repositioning framework."
    "	* SemMedDB version: 43"
    "	* UMLS version: "
    "	* Drug Central dump: 2022 August 22"
    ""
	"Usage: $SCRIPT [options] <command> [args]"
	""
	"Sample Commands"
	"bash $SCRIPT -H yourServerName -p 5432 -u yourUserName -a your_UMLS_API_Key" 
	"bash $SCRIPT --host yourServerName --port 5432 --user yourUserName"
	""
	"Options:"
	"	--host, -H			postgreSQL host name"
    "   --pass, -P			postgreSQL password"
	"	--port, -p			postgreSQL port number"
	"	--user, -u			postgreSQL user login"
	"	--apikey, -a		UMLS apikey"
    "   --umls_date, -D     UMLS version date"
    "   --sem_ver, -v       SemMedDB version"
	"	--help, -h			optional, usage information"
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
    --options a:D:H:P:u:p:v:h:: \
    --long apikey:,umls_date:,host:,pass:,user:,port:,sem_ver:,help::\
    --name 'download_requirements' -- "$@"
    )
    
if [ $? != 0 ] ; then echo "Terminating..." >&2 ; exit 1 ; fi

APIKEY=
PORT=
PASS=
HOST=
USER=
UMLS_DATE=
SEM_VER=

eval set --"$TEMP"
while true; do
    case "$1" in
        -h| --help)
            Help; exit ;;
        -a| --apikey) 
            APIKEY="$2"; 
            # echo "APIKEY:    $APIKEY";
            shift 2 ;;
        -D| --umls_date)
            UMLS_DATE="$2";
            # echo "UMLS_DATE: $UMLS_DATE";
            shift 2 ;;
        -H| --host) 
            HOST="$2";
            # echo "HOST:      $HOST";
            shift 2 ;;
        -P| --pass) 
            PASS="$2";
            # echo "PASS:      $PASS";
            shift 2 ;;
        -p| --port)
            PORT="$2";
            # echo "PORT:      $PORT";
            shift 2 ;;
        -v| --sem_ver)
            SEM_VER="$2";
            # echo "SEM_VER:   $SEM_VER";
            shift 2 ;;
        -u| --user) 
            USER="$2";
            # echo "USER:      $USER";
            shift 2 ;;
        -- ) shift; break ;; # allows you to  break loop once all options exhausted
        *) echo "ERROR: Invalid Option"; Help; exit 1;;
    esac
done

#
# Check for missing inputs
#
if [[ -z $APIKEY || -z $HOST || -z $PASS || -z $USER || -z $PORT ]]; then
    echo "ERROR: Missing APIKEY or HOST or PASS or USER or PORT input"
    echo "Please provide:"
    if [[ -z $APIKEY ]];then echo "    APIKEY (-a)"; fi
    if [[ -z $HOST ]]; then echo "    HOST (-H)"; fi
    if [[ -z $PASS ]];then echo "    PASS (-P)"; fi
    if [[ -z $PORT ]]; then echo "    PORT (-p)"; fi
    if [[ -z $USER ]]; then echo "    USER (-u)"; fi
    if [[ -z $UMLS_DATE ]]; then echo "    UMLS_DATE (-D)"; fi
    if [[ -z $SEM_VER ]]; then echo "    SEM_VER (-v)"; fi
    echo "See Help (-h) for more information"
    exit 1
fi

# Get UMLS
cd ./scripts
bash download_umls.sh $APIKEY $UMLS_DATE

# Get Semmed
bash download_semmeddb.sh $APIKEY $SEM_VER $UMLS_DATE

# Get Semmed doc types
bash get_semtype_files.sh 

# Get Drug Central
bash download_drugcentral.sh --host $HOST --port $PORT --user $USER --pass $PASS

# Get baseline
bash download_baseline.sh
