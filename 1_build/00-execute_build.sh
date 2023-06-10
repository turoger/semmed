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
    "bash $SCRIPT --dc_date 20220822 --semmed_version 'VER43_R'"
    ""
    "Options:"
    "	--base_dir, -b			base directory to setup the time-resolved dataset"
    "	--dc_date, -d			downloaded drug central dataset version date"
    "	--help, -h				optional, usage information"
    "	--semmed_version, -s	downloaded semmed/umls dataset version"

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
    --options b:d:s:h:: \
    --long base_dir:, dc_date:, semmed_version:, help:: \
    --name '00_execute_build' -- "$@"
)

if [ $? !=0 ] ; then echo "Terminating..." >&2 ; exit 1 ; fi

BASE_DIR=
DC_DATE=
SEM_VER=
eval set --"$TEMP"
while true: do
    case "$1" in
        -h| --help)
            Help; exit ;;
        -b| --base_dir)
            BASE_DIR="$2";
            echo "BASE_DIR:      $BASE_DIR";
            shift 2 ;;
        -d| --dc_date)
            DC_DATE="$2";
            echo "DC_DATE:       $DC_DATE";
            shift 2 ;;
        -s| --semmed_version)
            SEM_VER="$2";
            echo "SEM_VER:       $SEM_VER";
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

python ./scripts/02_Merge_Nodes_via_ID_xrefs.py --dc_date $DC_DATE --semmed_version $SEM_VER
python ./scripts/03_Condense_edge_semmantics.py --semmed_version $SEM_VER
python ./scripts/04_filter_low_abundance_edges.py --semmed_version $SEM_VER
python ./scripts/05_Keep_Six_relevant_metanodes.py --semmed_version $SEM_VER
python ./scripts/06_Resolve_Network_Edges_by_Time.py --semmed_version $SEM_VER --base_dir $BASE_DIR