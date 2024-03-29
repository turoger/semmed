#!/bin/bash

SCRIPT='build.sh'
#
# Help Message
#
Help()
{
	local message='$1'
	local txt=(
	"Description: Pipeline to Downloads SemMedDB, UMLS, and Drug Central to create a time-resolved drug repositioning framework to better model knowledge graph completion algorithm performance on predicting unseen indications."
    ""
    "The base knowledge graph is built off SemMedDB and is extended by approved drug-disease indications from DrugCentral as highlighted in Mayers et. al (2019). This repository builds off the original by speeding up the original code using the polars library, conducting bug fixes and streamlining the pipeline."
    ""
    "The data spans from 1950-2023, where a training set are triples that exist prior to a certain date, and the testing set are triples that exist future of said date. This time-resolved dataset consists of 6 metanodes (types): Chemicals & Drugs, Disorders,Genes & Molecular Sequences, Anatomy, Physiology, and Phenomena."
    ""
    "The pipeline is broken down into 2 main parts: 0_prepare and 1_build. A third part 2_ml, is in development. In order to use this pipeline, you *must* have a UMLS API key. You can obtain one by registering at https://uts.nlm.nih.gov/uts/signup-login. Additionally, you must have access to a PostgreSQL database to process some parts of the data."
    ""
	"Usage: $SCRIPT [options] <command> [args]"
	""
	"Sample Commands"
	"bash $SCRIPT -H yourServerName -p 5432 -u yourUserName -a your_UMLS_API_Key" 
	"bash $SCRIPT --host yourServerName --port 5432 --user yourUserName"
	""
	"Options:"
	"   --apikey, -a            UMLS apikey"
	"   --host, -H              postgreSQL host name"
    "   --pass, -P              postgreSQL password"
	"   --port, -p              postgreSQL port number"
	"   --user, -u              postgreSQL user login"
	"   --help, -h              usage information"
    "   --base_dir, -b          base directory to setup the time-resolved dataset; defaults to '../data/time_networks-6_metanode'"
    "   --semmed_ver, -v        optional, downloaded semmed/umls dataset version; defaults to 'VER43_R'"
    "   --umls_date, -D         optional, downloaded semmed dataset version date; defaults to '2023AA'"
    "   --dc_date, -d           optional, downloaded drug central dataset version date; defaults to '11012023'"
	"   --drop_neg, -n          optional, data generated will drop negative relations with flag; default unflagged, will keep negative relations"
    "   --convert_neg, -c       optional, convert negative relations to bidirectional relations; defaults to unflagged"
    "   --include_direction, -i optional, include direction in the dataset; defaults to unflagged"
    "   --include_time, -x      optional, include time in the dataset; defaults to unflagged"
    "   --split_hpo, -o         optional, split the dataset for hyperparameter optimization; defaults to unflagged"
    "   --split_ttv, -t         optional, split the dataset for training, testing, and validation; defaults to unflagged"
    "   --hpo_year, -y          optional, year to split the dataset for hyperparameter optimization; defaults to 1987"
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
    --options a:b:c::d:D:i:h::H:n::o::p:P:t::u:v:x::y: \
    --long apikey:,base_dir:,convert_neg::,dc_date:,umls_date:,include_direction:,help::,host:,drop_neg::,split_hpo::,port:,pass:,split_ttv::,user:,semmed_ver:,include_time::,hpo_year:\
    --name 'build' -- "$@"
    )
    
if [ $? != 0 ] ; then echo "Terminating..." >&2 ; exit 1 ; fi

APIKEY=
HOST=
PASS=
PORT=
USER=
BASE_DIR=
DC_DATE=
SEM_VER=
UMLS_DATE=
DROP_NEGATIVE_EDGES=0
CONVERT_NEG=0
INCLUDE_DIRECTION=0
HPO=0
TTV=0
TIME=0
HPO_YEAR=0
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
        -P| --pass) 
            PASS="$2";
            echo "PASS:      $PASS";
            shift 2 ;;
        -p| --port)
            PORT="$2";
            echo "PORT:      $PORT";
            shift 2 ;;
        -u| --user) 
            USER="$2";
            echo "USER:      $USER";
            shift 2 ;;
        -b| --base_dir) 
            BASE_DIR="$2";
            echo "BASE_DIR: $BASE_DIR";
            shift 2 ;;
        -v| --semmed_ver) 
            SEM_VER="$2";
            echo "SEM_VER:  $SEM_VER";
            shift 2 ;;
        -d| --dc_date) 
            DC_DATE="$2";
            echo "DC_DATE:  $DC_DATE";
            shift 2 ;;
        -D| --umls_date)
            UMLS_DATE="$2";
            echo "UMLS_DATE: $UMLS_DATE";
            shift 2;;
        -n| --drop_neg)
            DROP_NEGATIVE_EDGES=1;
            echo "DROP_NEGATIVE_EDGES: $DROP_NEGATIVE_EDGES";
            shift 2;;
        -c| --convert_neg)
            CONVERT_NEG=1;
            echo "CONVERT_NEG: $CONVERT_NEG";
            shift 2;;
        -i| --include_direction)
            INCLUDE_DIRECTION=1;
            echo "INCLUDE_DIRECTION: $INCLUDE_DIRECTION";
            shift 2;;
        -o| --split_hpo)
            HPO=1;
            echo "HPO:      $HPO";
            shift 2;;
        -t| --split_ttv)
            TTV=1;
            echo "TTV:      $TTV";
            shift 2;;
        -x| --include_time)
            TIME=1;
            echo "TIME:     $TIME";
            shift 2;;
        -y| --hpo_year)
            HPO_YEAR="$2";
            echo "HPO_YEAR: $HPO_YEAR";
            shift 2;;
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
    echo "See Help (-h) for more information"
    exit 1
fi

#
# check for empty variables, supply some defaults
#
if [ -z "${BASE_DIR}" ]; then BASE_DIR="../data/time_networks-6_metanode";fi
if [ -z "${DC_DATE}" ]; then DC_DATE="11012023";fi # old version "20220822"
if [ -z "${UMLS_DATE}" ]; then UMLS_DATE="2023AA";fi
if [ -z "${SEM_VER}" ]; then SEM_VER="VER43_R";fi
if [ -z "${HPO_YEAR}" ]; then HPO_YEAR=1987;fi

#
# Setup virtual environment. If it already exists, skip
#
echo "Creating virtual environment"
eval "$(conda shell.bash hook)" # instantiates conda hook
VENV=$(conda env list | grep mini_semmed | awk '{print $1}')
if ! [ -n "$VENV" ]; then
    conda env create -f mini_env.yml -y
    echo "Virtual environment created"
else
    echo "Virtual environment already exists"
fi

#
# Download Requirements
#
echo "Downloading required files"
cd ./0_prepare/
bash download_requirements.sh --host $HOST --port $PORT --user $USER --pass $PASS --apikey $APIKEY --umls_date $UMLS_DATE --sem_ver $SEM_VER --dc_date $DC_DATE
# Pre-processing
echo "Preparing SEMMED Heterogenous Network"
echo "... loading conda environment (mini_semmed)"
 conda run -n mini_semmed --no-capture-output python preprocessing.py --semmed_version $SEM_VER --umls_date $UMLS_DATE
# conda run -n mini_semmed python ./scripts/01_initial_data_clean.py --semmed_version $SEM_VER
# conda run -n mini_semmed python ./scripts/02_id_to_publication_year.py --semmed_version $SEM_VER
# conda run -n mini_semmed python ./scripts/03_umls_cui_to_mesh_descriptorID.py --semmed_version $SEM_VER  --umls_date $UMLS_DATE
# conda run -n mini_semmed python ./scripts/04_parse_mesh_data.py --umls_date $UMLS_DATE
# conda run -n mini_semmed python ./scripts/05_mesh_id_to_name_via_umls.py --umls_date $UMLS_DATE


#
# Prepare
#
cd ../1_build

# build Heterogenous Network
echo "Building SEMMED Heterogenous Network"

# check flags for dropping negative edges
if [[ $DROP_NEGATIVE_EDGES -eq 1 ]]; then
    DROP_NEGATIVE_EDGES="--drop_negative_relations"
else
    DROP_NEGATIVE_EDGES=""
fi

# check flags for converting negative edges
if [[ $CONVERT_NEG -eq 1 ]]; then
    CONVERT_NEG="--convert_negative_relations"
else
    CONVERT_NEG=""
fi

# check flags for including direction
if [[ $INCLUDE_DIRECTION -eq 1 ]]; then
    INCLUDE_DIRECTION="--include_direction"
else
    INCLUDE_DIRECTION=""
fi

# check flags for including time in the dataset split
if [[ $TIME -eq 1 ]]; then
    TIME="--include_time"
else
    TIME=""
fi

# check flags for splitting dataset for hyperparameter optimization
# where train/test/valid is all on time before a certain year
if [[ $HPO -eq 1 ]]; then
    HPO="--split_hpo"
else
    HPO=""
fi
# check flags for splitting dataset for train/test/valid, if false just split train/test
if [[ $TTV -eq 1 ]]; then
    TTV="--split_ttv"
else
    TTV=""
fi


#
# run rest of the pipeline. Please check flags to make sure version drugcentral and semmed downloaded matches
# 
conda run -n mini_semmed --no-capture-output python building.py --semmed_version $SEM_VER $DROP_NEGATIVE_EDGES $CONVERT_NEG $INCLUDE_DIRECTION $TIME $HPO $TTV --base_dir $BASE_DIR --dc_date $DC_DATE --hpo_year $HPO_YEAR
# conda run -n mini_semmed python ./scripts/01_build_hetnet_polars.py --semmed_version $SEM_VER $DROP_NEGATIVE_EDGES $CONVERT_NEG $INCLUDE_DIRECTION
# conda run -n mini_semmed python ./scripts/02_Merge_Nodes_via_ID_xrefs_polars.py --dc_date $DC_DATE --semmed_version $SEM_VER
# conda run -n mini_semmed python ./scripts/03_Condense_edge_semmantics_polars.py --semmed_version $SEM_VER
# conda run -n mini_semmed python ./scripts/04_filter_low_abundance_edges_polars.py --semmed_version $SEM_VER
# conda run -n mini_semmed python ./scripts/05_Keep_Six_relevant_metanodes_polars.py --semmed_version $SEM_VER
# conda run -n mini_semmed python ./scripts/06_Resolve_Network_Edges_by_Time_polars.py --semmed_version $SEM_VER --base_dir $BASE_DIR
# conda run -n mini_semmed python ./scripts/07_Build_data_split.py --semmed_version $SEM_VER --base_dir $BASE_DIR --hpo_year $HPO_YEAR $HPO $TTV $TIME