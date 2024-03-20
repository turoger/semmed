#!/bin/bash

# Script from https://documentation.uts.nlm.nih.gov/automating-downloads.html
# 1. Add your API key from https://uts.nlm.nih.gov/uts/profile

export CAS_LOGIN_URL=https://utslogin.nlm.nih.gov/cas/v1/api-key

SCRIPT='download_from_umls_api.sh'

# Help message
Help(){
	local message='$1'
	local txt=(
	"Description: Contacts UMLS to download requested link."
	"Usage: $SCRIPT [options] <command> [args]"
	""
	"Sample Commands"
	"bash $SCRIPT -a 1234-5678-90 -l https://download.something.from.nih.gov"
	"bash $SCRIPT --apikey 1234-5678-90 --link https://download.something.from.nih.gov"
	""
	"Options:"
	"	--apikey, -a	UMLS account api key"
	"	--help, -h	prints help"
	"	--link, -l	file requested from the UMLS website"

	)
	printf "%s\n" "${txt[@]}"
}


#
# 2. assign vars with short or long form flags
#
TEMP=$(getopt \
    --options l:a:h:: \
    --long link:,apikey:,help:: \
    --name 'download_from_umls_api' -- "$@"
    )
if [ $? != 0 ] ; then echo "Terminating..." >&2 ; exit 1; fi

DOWNLOAD_URL=
APIKEY=
eval set --"$TEMP"
while true; do
    case "$1" in
        -h| --help)
            Help; exit ;;
        -l| --link)
            DOWNLOAD_URL="$2";
            echo "DOWNLOAD_URL:      $DOWNLOAD_URL";
            shift 2 ;;
        -a| --apikey)
            APIKEY="$2";
            echo "APIKEY:      $APIKEY";
            shift 2 ;;
        -- ) shift; break ;;
        *) echo "ERROR: Invalid Option"; Help; exit 1 ;;
    esac
done

#
# 3. Check for missing inputs
#
if [[ -z $APIKEY || -z $DOWNLOAD_URL ]]; then
    echo "ERROR: Missing APIKEY or DOWNLOAD_URL"
    echo "Please provide:"
    if [[ -z $APIKEY ]];then echo "    APIKEY (-a)"; fi
    if [[ -z $DOWNLOAD_URL ]]; then echo "    LINK (-l)"; fi
    echo "See Help (-h) for more information"
    exit 1
fi

#
# 4. Get your Ticket and your TGT value
#

echo "Retrieving Ticket Granting Ticket"
TGT=$(curl -d "apikey="$APIKEY -H "Content-Type: application/x-www-form-urlencoded" -X POST https://utslogin.nlm.nih.gov/cas/v1/api-key)

TGTTICKET=$(echo $TGT | tr "=" "\n")

for TICKET in $TGTTICKET
do
	if [[ "$TICKET" == *"TGT"* ]]; then
      		SUBSTRING=$(echo $TICKET| cut -d'/' -f 7)
		TGTVALUE=$(echo $SUBSTRING | sed 's/.$//')
	fi
done

#
# 5. Get service given your TGT value
#
echo "Ticket Granting Ticket: $TGTVALUE"
STTICKET=$(curl -d "service="$DOWNLOAD_URL -H "Content-Type: application/x-www-form-urlencoded" -X POST https://utslogin.nlm.nih.gov/cas/v1/tickets/$TGTVALUE)

echo "Service Ticket: $STTICKET"
# (-C) continue-at Resumed transfer offset, (-J) remote-header-name, (-O) remote-name, (-L) location follow redirects 
curl -c cookie.txt -b cookie.txt -JLO $DOWNLOAD_URL?ticket=$STTICKET
rm cookie.txt
