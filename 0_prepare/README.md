# Preparation for building the time-resolved SemmedDB network

This folder contains the scripts and notebooks needed to download and do some inital processing on
all various data sources for this project.

## To run

Although a lot of scripts in this directory can be run in any order, a few have dependencies.  For Best results
run the shell script `download_requirements.sh` first, then run `preprocessing.py`.

To ensure all scripts run properly, please enable the virtual enviornment `mini_semmed` before running.


### The following script must be run for the rest of the pipline to work

* `./download_requirements.sh`
* This script will download all the appropriate files automatically.
* Please add your personal UMLS apikey as well as PostgreSQL server information as flags to the script before running it.

```
bash download_requirements.sh --apikey [umls apikey] --dc_date [drugcentral version] \
--host [psql hostname] --port [psql port] --user [psql username] --pass [psql password]

bash download_requirements.sh -a [umls apikey] -d [drugcentral version] -H [psql hostname] \
-p [psql port] -u [psql username] -P [psql password]
```

#### Requirements

* `./scripts/download_drugcentral.sh` requires PostgreSQL to be installed on the system.
	* This dump was created with PostgreSQL 10, however it appears to work with 9.5 as well.
	* To install PostgreSQL, please follow instructions outlined here: https://www.postgresql.org/download/linux/ubuntu/
	* `sudo service postgresql start` to start PostgreSQL service
	* `sudo service postgresql stop` to kill the service 
	* add your psql server information to the appropriate variables in `./download_requirements.sh`
* `pv` is also utilized to view the progress of psql manipulations

### After running the scripts above, run the python script
* `./preprocessing.py`
* This script will extract publication dates and associate them to specific pubmed ids.
```
conda run -n mini_semmed --no-capture-output python preprocessing.py --semmed_ver 'VER43_R' \
 --umls_date '2023AA' # if you want to run non-interactively

python preprocessing.py -v [semmed version] -D [umls year code] # otherwise activate an environment first
```
