#!/bin/bash

SCRIPT='00-execute_build.sh'
#
# Help Message
#
Help()
{
    local message='$1'
    local txt=(
    "Description: Build the time-resolved drug repositioning framework"
    ""
    "Usage: bash $SCRIPT [options] <command> [args]"
    ""
    "Sample Command"
    "bash $SCRIPT --dc_date 20220822 --semmed_version 'VER43_R' "
    ""
    "Options:"
    "   --base_dir, -b                          (str), base directory to setup the time-resolved dataset"
    "   --dc_date, -d                           (str), downloaded drug central dataset version date"
    "   --help, -h                              optional, usage information"
    "   --semmed_version, -n                    (str), downloaded semmed/umls dataset version"
    "   --split_hyperparameter_optimization, -p {True|False}, split the dataset for hyperparameter optimization"
    "   --split_train_test_valid, -s            {True|False}, True, split the dataset for training, testing, and validation. Otherwise split the dataset to training and testing"
    "   --include_time, -t                      {True|False}, include time in the dataset"
    "   --hpo_year, -y                          (int), year to split the dataset for hyperparameter optimization"

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
    --options b:d:n:p:s:t:y:h:: \
    --long base_dir:,dc_date:,semmed_version:,split_hyperparameter_optimization:,split_train_test_valid:,include_time:,hpo_year:,help:: \
    --name '00-execute_build' -- "$@"
    )
    
if [ $? != 0 ] ; then echo "Terminating..." >&2 ; exit 1 ; fi

BASE_DIR=
DC_DATE=
SEM_VER=
HPO=
TTV=
TIME=
HPO_YEAR=
#
# check for empty variables, supply some defaults
#
if [ -z "${BASE_DIR}"]; then BASE_DIR="../data/time_networks-6_metanode";fi
if [ -z "${DC_DATE}"]; then DC_DATE="20220822";fi
if [ -z "${SEM_VER}"]; then SEM_VER="VER43_R";fi
if [ -z "${HPO}"]; then HPO=1;fi
if [ -z "${TTV}"]; then TTV=1;fi
if [ -z "${TIME}"]; then TIME=1;fi
if [ -z "${HPO_YEAR}"]; then HPO_YEAR=1987;fi

eval set --"$TEMP"
while true; do
    case "$1" in
        -h| --help)
            Help; exit ;;
        -b| --base_dir)
            BASE_DIR="$2";
            echo "BASE_DIR:    $BASE_DIR";
            shift 2 ;;
        -d| --dc_date)
            DC_DATE="$2";
            echo "DC_DATE:    $DC_DATE";
            shift 2 ;;
        -n| --semmed_version)
            SEM_VER="$2";
            echo "SEM_VER:    $SEM_VER";
            shift 2 ;;
        -p| --split_hyperparameter_optimization)
            HPO="$2";
            echo "HPO:    $HPO";
            shift 2 ;;
        -s| --split_train_test_valid)
            TTV="$2";
            echo "TTV:    $TTV";
            shift 2 ;;
        -t| --include_time)
            TIME="$2";
            echo "TIME:    $TIME";
            shift 2 ;;
        -y| --hpo_year)
            HPO_YEAR="$2";
            echo "HPO_YEAR:    $HPO_YEAR";
            shift 2 ;;
        --) shift; break ;;
        *) echo  "ERROR: Invalid Option"; Help; exit 1;;
    esac
done

#
# Check for missing inputs
#
if [[ -z $BASE_DIR || -z $DC_DATE || -z $SEM_VER ]]; then
    echo "ERROR: Missing BASE_DIR or DC_DATE or SEM_VER"
    echo "Please provide:"
    if [[ -z $BASE_DIR ]];then echo "    BASE_DIR (-b | --base_dir)"; fi
    if [[ -z $DC_DATE ]]; then echo "    DC_DATE (-d | --dc_date)"; fi
    if [[ -z $SEM_VER ]];then echo "    SEM_VER (-s | --sem_version)"; fi
    echo "See Help (-h | --help) for more information"; Help
    exit 1
fi


python ./scripts/01_build_hetnet_polars.py --semmed_version $SEM_VER --convert_negative_relations True
python ./scripts/02_Merge_Nodes_via_ID_xrefs_polars.py --dc_date $DC_DATE --semmed_version $SEM_VER
python ./scripts/03_Condense_edge_semmantics_polars.py --semmed_version $SEM_VER
python ./scripts/04_filter_low_abundance_edges_polars.py --semmed_version $SEM_VER
python ./scripts/05_Keep_Six_relevant_metanodes_polars.py --semmed_version $SEM_VER
python ./scripts/06_Resolve_Network_Edges_by_Time_polars.py --semmed_version $SEM_VER --base_dir $BASE_DIR
python ./scripts/07_Build_data_split.py --semmed_version $SEM_VER --base_dir $BASE_DIR --split_hyperparameter_optimization $HPO --split_train_test_valid $TTV --include_time $TIME --hpo_year $HPO_YEAR