#!/bin/bash

# Script from https://documentation.uts.nlm.nih.gov/automating-downloads.html
# 1. Add your API key from https://uts.nlm.nih.gov/uts/profile


export apikey=
export DOWNLOAD_URL=$1
export CAS_LOGIN_URL=https://utslogin.nlm.nih.gov/cas/v1/api-key


#
# 2. Check for Errors
## Is API Key missing?
if [ -z "$apikey" ]; then echo " Please enter you api key in download_from_umls_api.sh "
	   exit
fi

## Is there a missing variable?
if [ $# -eq 0 ]; then echo "Usage: download_from_umls_api.sh  download_url "
	echo "  e.g.   download_from_umls_api.sh https://download.nlm.nih.gov/umls/kss/rxnorm/RxNorm_full_current.zip"
        echo "         download_from_umls_api.sh https://download.nlm.nih.gov/umls/kss/rxnorm/RxNorm_weekly_current.zip"
	exit
fi


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
