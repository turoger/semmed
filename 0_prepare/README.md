# Preparation for building the time-resolved SemmedDB network

This folder contains the scripts needed to download and process data sources to build the time-resolved biomedical knowledge graph. The scripts in the `./scripts` folder expand off prior work done by Mayers et al., and the notebooks associated with these scripts can be found [here](https://github.com/mmayers12/semmed/tree/master/prepare).

## Requirements

`./scripts/download_drugcentral.sh` requires PostgreSQL to be installed on the system. `pv` is also utilized to view the progress of psql manipulations
* This dump was created with PostgreSQL 10, however it also works with 9.5.
* To install PostgreSQL, please follow instructions outlined here: https://www.postgresql.org/download/linux/ubuntu/

```bash
sudo service postgresql start 	# to start PostgreSQL service
sudo service postgresql stop 	# to kill the service 
```

## To run

The easiest way to execute the scripts is to use the shell scripts `download_requirements.sh` and `preprocessing.py` (and their associated flags).

### `./download_requirements.sh`
* This script will download all the appropriate files automatically.
* Please add your personal UMLS apikey as well as PostgreSQL server information as flags to the script before running it.

### `preprocessing.py`
* This script will do preliminary preprocessing on the semmed dataset by cleaning broken entries, generate and fix entity mapping

```
# run in your shell to download files
bash download_requirements.sh \
	--host <psql-host> \
	--port <psql-port> \
	--user <psql-user> \
	--pass <psql-password> \
	--apikey <umls-account-key> \
	--umls_date <umls-date> \
	--sem_ver <semmed-version> \
	--dc_date <drugcentral-date>

# then execute preprocessing script
mamba run -n mini_semmed2 --no-capture-output python preprocessing.py \
	--semmed_ver <semmed-version> \
	--umls_date <umls-date>
```

### to run manually
* to use the invidual download scripts in `./scripts` and use the appropriate flagging
* to use the individual python scripts, activate the virtual environment with `mamba activate mini_semmed2` and run each of the scripts.