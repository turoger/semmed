# Introduction

This repository contains code to streamline the creation of a time-resolved drug repurposing heterogenous network benchmark. This dataset aims to better model knowledge graph completion algorithm performance on predicting unseen future approved drug-disease indications. 

The knowledge graph is derived off [SemMedDB](https://skr3.nlm.nih.gov/SemMedDB/) and is extended by approved drug-disease indications from [DrugCentral](http://drugcentral.org/) as highlighted in [Mayers et. al (2019)](https://www.ncbi.nlm.nih.gov/pmc/articles/PMC6907279/). 

Data is segmented by year. Each year is segmented into a train and test set; the train set is comprised of triples prior to the given year, and the test set is comprised of approved drug-disease triples after the given year. There are a total of 73 datasets spanning from 1950 to 2023, 6 metanodes (types), and 32 unique relations.

To build the dataset in this repository, a UTS license/account is required. [You can apply for one here.](https://uts.nlm.nih.gov//license.html). Additionally, you must have access to a PostgreSQL database for initial processing.

# Usage

Building the dataset can be done by calling the `build.sh` script with appropriate flags as seen in the example below.

```
bash build.sh --apikey [UMLS apikey] --host [psql hostname] --pass [psql password] --port [psql port] --user [psql user]
```

The `timeresolvedkg.py` file is a pykeen dataset class that will run the entire pipeline (if you supply the minimum flagged information) after adding the file path to the `setup.cfg` and rebuilding the pykeen library with `pip install .`. 

# Installation
In order to use the repository scripts manually, a python environment should be setup and activated. The previous script automates the construction of a conda environment to run the independent scripts. 

## Setting up the python environment.

Use anaconda or mamba with environment.yml to run this code. A minimum environment can be created using the `mini_env.yml` file.

use `conda env create -f mini_semmed.yml -y` to install the enviornment. Then use `source activate mini_semmed` to start the environment.

## Requirements

In addition to the virtual environment, the following programs and package required to run all scripts
in this repo

- `pv` - progress viewer for opening text
- `psql` - PostgreSQL database for Drugcentral Data Dump parsing

# Running the Pipeline

## 1. prepare

The prepare folder contains the scripts and notebooks needed to download semmedDB and other mapping
files used for this repo, as well as some pre-processing steps. Run `bash download_requirements.sh` with flags to download the appropriate files. Run `python preprocessing.py` to generate files for downstream data cleaning. 

## 2. build

The build folder contains the notebooks that builds the time-resolved knowledge graph. Run `python building.py` to create the knowledge graph and appropriate splits by using the flagging options.

## 3. ml

The ml folder contains scripts to run a machine learning pipeline on the generated heterogenous network, as well as notebooks for the analysis of these results. These are a work in progress

# Other Sections

## tools

The tools folder cotains some helpful scripts, useful for processing  project data.

## data

All data will be downloaded and written to this folder.  Contains a couple small starter files important for running and speeding up the pipeline.

# References

## Time-resolved network

Michael Mayers, Tong Shu Li, Núria Queralt-Rosinach, Andrew I. Su. (2019) Time-resolved evaluation of compound repositioning predictions on a text-mined knowledge network. BMC Bioinformatics 20, 653. [https://doi.org/10.1186/s12859-019-3297-0](https://doi.org/10.1186/s12859-019-3297-0)

## SemMedDB

Kilicoglu, H, Shin D, Fiszman M, Rosemblat G, Rindflesch TC. (2012) SemMedDB: A PubMed-scale repository of biomedical semantic predications. Bioinformatics, 28(23), 3158-60.

## DrugCentral

Oleg Ursu  Jayme Holmes  Jeffrey Knockel  Cristian G. Bologa  Jeremy J. Yang Stephen L. Mathias  Stuart J. Nelson  Tudor I. Oprea. (2017) DrugCentral: online drug compendium. Nucleic Acids Research, 45(D1) D932–D939. [https://doi.org/10.1093/nar/gkw993](https://doi.org/10.1093/nar/gkw993)
