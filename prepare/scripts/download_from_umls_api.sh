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


# 2. check if missing options
if [ $# -eq 0 ];
then
	echo "Please check for missing options"
	echo ""
	Help

elif [ $# -gt 4 ];
then
	echo "Too many inputs: $# given"
	echo ""
	Help
fi


# turn inputs into an array
vars=($@)

# store options
for i in "${!vars[@]}"
do
	if [[ "${vars[i]}" = '--link' || "${vars[i]}" = '-l' ]]
	then
		echo "DOWNLOAD_URL: ${vars[i+1]}"
		export DOWNLOAD_URL="${vars[i+1]}"	
	elif [[ "${vars[i]}" = '--apikey' || "${vars[i]}" = '-a' ]]
	then
		echo "apikey: ${vars[i+1]}"
		export apikey="${vars[i+1]}"
	elif [[ "${vars[i]}" = '--help' || "${vars[i]}" = '-h'  ]]
	then
		Help
		exit 0
	
	fi
done

#
# 3. Get your Ticket and your TGT value
#
TGT=$(curl -d "apikey="$apikey -H "Content-Type: application/x-www-form-urlencoded" -X POST https://utslogin.nlm.nih.gov/cas/v1/api-key)

TGTTICKET=$(echo $TGT | tr "=" "\n")

for TICKET in $TGTTICKET
do
	if [[ "$TICKET" == *"TGT"* ]]; then
      		SUBSTRING=$(echo $TICKET| cut -d'/' -f 7)
		TGTVALUE=$(echo $SUBSTRING | sed 's/.$//')
	fi
done

#
# 4. Get service given your TGT value
#
echo $TGTVALUE
STTICKET=$(curl -d "service="$DOWNLOAD_URL -H "Content-Type: application/x-www-form-urlencoded" -X POST https://utslogin.nlm.nih.gov/cas/v1/tickets/$TGTVALUE)

echo $STTICKET
curl -c cookie.txt -b cookie.txt -L -O -J $DOWNLOAD_URL?ticket=$STTICKET
rm cookie.txt
