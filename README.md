# Introduction

This repository contains code to streamline the creation of a time-resolved drug repurposing heterogenous network benchmark. This dataset aims to better model knowledge graph completion algorithm performance on predicting unseen future approved drug-disease indications. 

The knowledge graph is derived off [SemMedDB](https://skr3.nlm.nih.gov/SemMedDB/) and is extended by approved drug-disease indications from [DrugCentral](http://drugcentral.org/) as highlighted in [Mayers et. al (2019)](https://www.ncbi.nlm.nih.gov/pmc/articles/PMC6907279/). This repository updates the original pipeline by replacing notebook executions with python scripts, refactoring code to decrease dependencies and increase speed, adds option flagging, and various bug fixes. 

Data is segmented by year. Each year is segmented into a train and test set (or train, test, and valid set); the train set is comprised of triples prior to the given year, and the test set is comprised of approved drug-disease triples after the given year. There are a total of 73 datasets spanning from 1950 to 2023, 6 metanodes (types), and 32 unique relations.

To build the dataset in this repository, a UTS license/account is required. You can apply for one [here](https://uts.nlm.nih.gov/uts/signup-login) (may take up to 3 days to be approved). Additionally, you must have access to a PostgreSQL database for initial processing.

A convenient script to integrate the dataset into PyKEEN, a knowledge graph embedding framework and library, can be found [here](./timeresolvedkg.py). Installation details can be found below. 

# Usage
The dataset can be generated automatically via `build.sh` or manually by stepping through the scripts of interest, sequentially. Either way, there are some required softwares needed prior to usage.

## Required
### postgres
* Please install `pv` and `postgresql` prior to running.
```
# on ubuntu
sudo apt-get update
sudo apt-get upgrade

sudo apt install pv postgresql
```

* Create a psql user and password
```
# login to psql, create a username and password as postgres, quit.
user@server:~$ sudo su postgres 
postgres:~$ createuser [myuser] --createdb --pwprompt
postgres:~$ exit
```
### conda/mamba
* Please install [mamba](https://mamba.readthedocs.io/en/latest/installation/mamba-installation.html) following the directions outlined in the link.

## Building the dataset for your own use
* Building the dataset can be done automatically by calling the `build.sh` script with appropriate flags as seen in the example below.

```
bash build.sh --apikey [UMLS apikey] --host [psql hostname] --pass [psql password] \
 --port [psql port] --user [psql user]
```

* When building, it is highly recommended to run the command in a persistent session using `screen` (deprecated) or `tmux` (preferred); even with all the tweaks, downloading files and pinging APIs still takes a while.

### Manual mode
* If you want to run each script by itself (or enjoy life on hard mode), first install the python environment using the `mini_env.yml` file. To do so, first ensure you have conda/mamba (preferred) installed (above), then use the following command in your favorite linux shell in the cloned directory:

```
mamba env create -f mini_env.yml -y
```

* and activate the environment with:
```
mamba activate mini_semmed2
```

## pykeen integration
To integrate the dataset with [pykeen](https://github.com/pykeen/pykeen), do the following things:
* clone the pykeen repository
* copy `timeresolvedkg.py` file in this repository into your local pykeen repo at`pykeen/datasets/timeresolvedkg.py`; this file is a pykeen dataset class that will run the entire pipeline (if you supply the minimum flagged information) 
* Add the file path to the `setup.cfg` entrypoint for pykeen. example:
```bash
[options.entry_points]
console_scripts = 
    pykeen = pykeen.cli:main
pykeen.datasets = 
    trkg = pykeen.datasets.timeresolvedkg:TimeResolvedKG
```
* Finally, rebuild the pykeen library with `pip install .`

# Organization

## 1. prepare

The prepare folder contains the scripts and notebooks needed to download semmedDB and other mapping.

files used for this repo, as well as some pre-processing steps. Run `bash download_requirements.sh` with flags to download the appropriate files. Run `python preprocessing.py` to generate files for downstream data cleaning. 

## 2. build

The build folder contains the notebooks that builds the time-resolved knowledge graph. Run `python building.py` to create the knowledge graph and appropriate splits by using the flagging options.

## tools

The tools folder cotains some helpful scripts, useful for processing project data.

## data

All data will be downloaded and written to this folder.  Contains a couple small starter files important for running and speeding up the pipeline.

# References

## Time-resolved network

Michael Mayers, Tong Shu Li, Núria Queralt-Rosinach, Andrew I. Su. (2019) Time-resolved evaluation of compound repositioning predictions on a text-mined knowledge network. BMC Bioinformatics 20, 653. [https://doi.org/10.1186/s12859-019-3297-0](https://doi.org/10.1186/s12859-019-3297-0)

## SemMedDB

Halil Kilicoglu,Dongwook Shin, Marcelo Fiszman, Graciela Rosemblat, Thomas C Rindflesch. (2012) SemMedDB: A PubMed-scale repository of biomedical semantic predications. Bioinformatics, 28(23), 3158-60. [https://doi.org/10.1093/bioinformatics/bts591](https://doi.org/10.1093/bioinformatics/bts591)

## DrugCentral

Oleg Ursu  Jayme Holmes  Jeffrey Knockel  Cristian G. Bologa  Jeremy J. Yang Stephen L. Mathias  Stuart J. Nelson  Tudor I. Oprea. (2017) DrugCentral: online drug compendium. Nucleic Acids Research, 45(D1) D932–D939. [https://doi.org/10.1093/nar/gkw993](https://doi.org/10.1093/nar/gkw993)
