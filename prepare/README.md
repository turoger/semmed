# Preparation for building the SemmedDB network

This folder contains the scripts and notebooks needed to download and do some inital processing on
all various data sources for this project.

## To run

Although a lot of scripts in this directory can be run in any order, a few have dependencies.  For Best results
run the shell scripts first, then run the ipython notebooks.

To ensure all scripts run properly, please enable the virtual enviornment before running


### The following scripts must be run for the rest of the pipline to work

1. `./download_requirements.sh`
2. `./download_baseline.sh`
3. `./get_semtype_files.sh`

### After running the scripts above, run the jupyter notebooks in order

## Requirments
* `./scripts/download_semmeddb.sh` uses the program pv to view progress as it converts the SQL dump to a .csv file

* `./scripts/download_umls.sh` requires a UMLS account api-key. Get your api-key by logging onto UMLS and going to your settings. Paste the key into the `apikey` variable in `./download_requirements.sh`

* `./scripts/download_drugcentral.sh` requires PostgreSQL to be installed on the system.
	* This dump was created with PostgreSQL 10, however it appears to work with 9.5 as well.
	* To install PostgreSQL, please follow instructions outlined here: https://www.postgresql.org/download/linux/ubuntu/
	* `sudo service postgresql start` to start PostgreSQL service
	* `sudo service postgresql stop` to kill the service 
	* add your psql server information to the appropriate variables in `./download_requirements.sh`
