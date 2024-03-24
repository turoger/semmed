# semmed

Code to create a time-based heterogenous network from the data in SemmedDB and perform a machine learning alorithm on the resulting network. This work streamlines the generation of the network.

The primary data source for the network is [SemMedDB](https://skr3.nlm.nih.gov/SemMedDB/). The gold standard used
for machine learning is derived from [DrugCentral](http://drugcentral.org/).

To use much of this repository, a UTS license/account is required. [You can apply for one here.](https://uts.nlm.nih.gov//license.html)

This repository is divided into several steps.  First, the python enviornment should be set up and activated

## Setting up the python environment.

Use anaconda with environment.yml to run this code.  After installing anaconda
use `conda env create -f > --file environment.yml` to install the enviornment. Then use
`source activate ml` to start the environment.

## Running the Pipeline

### 1. prepare

The prepare folder contains the scripts and notebooks needed to download semmedDB and other mapping
files used for this repo, as well as some pre-processing steps.

### 2. build

The build folder contains the notebooks that build the hetnet version of semmedDB

### 3. ml

The ml folder contains scripts to run the machine learning pipeline on the generated heterogenous network, as well as
notebooks for the analysis of these results.

### Other Sections

#### tools

The tools folder cotains some helpful scripts, useful for this project.

#### data

All data will be downloaded and written to this folder.  Contains a couple small starter files important
for running and speeding up the pipeline.

## Requirements

In addition to the anaconda environment, the following programs and package required to run all scripts
in this repo

- `pv` - progress viewer for opening text
- `psql` - PostgreSQL database for Drugcentral Data Dump parsing
- [UMLS Account](https://uts.nlm.nih.gov//license.html) - Required for downloading the UMLS Metathesaurs data.

## References

### Time-resolved network

Michael Mayers, Tong Shu Li, Núria Queralt-Rosinach, Andrew I. Su. (2019) Time-resolved evaluation of compound repositioning predictions on a text-mined knowledge network. BMC Bioinformatics 20, 653. [https://doi.org/10.1186/s12859-019-3297-0](https://doi.org/10.1186/s12859-019-3297-0)

### SemMedDB

Kilicoglu, H, Shin D, Fiszman M, Rosemblat G, Rindflesch TC. (2012) SemMedDB: A PubMed-scale repository of biomedical semantic predications. Bioinformatics, 28(23), 3158-60.

### DrugCentral

Oleg Ursu  Jayme Holmes  Jeffrey Knockel  Cristian G. Bologa  Jeremy J. Yang Stephen L. Mathias  Stuart J. Nelson  Tudor I. Oprea. (2017) DrugCentral: online drug compendium. Nucleic Acids Research, 45(D1) D932–D939. [https://doi.org/10.1093/nar/gkw993](https://doi.org/10.1093/nar/gkw993)
