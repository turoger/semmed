# semmed

Code to create a hetnet from the data in SemmedDB

To use much of this repo, a UMLS liscence/account is required. [You can apply for one here.](https://uts.nlm.nih.gov//license.html)

This repo is divided into several steps.  First, the python enviornment should be set up and activated

## Setting up the python environment.

Use anaconda with enviornment.yml to run this code.  After installing anaconda
use `conda env create envionment.yml` to install the enviornment. Then use
`source activate ml` to start the enviornmnet.

## Running the Pipeline

### 1. prepare

The prepare folder contains the scripts and notebooks needed to download semmedDB and other mapping
files used for this repo, as well as some pre-processing steps.

### 2. build

The build folder contains the notebooks that build the hetnet version of semmedDB

### 3. ml

The ml folder contains scripts to run the machine learning pipeline on the generated hetnet, as well as
notebooks for the analysis of these results.

### Other Sections


#### tools

The tools folder cotains some helpful scripts, useful for this project.

#### data

All data will be downloaded and written to this folder.  Contains a couple small starter files important
for running the pipeline.

## Requirements

In addition to the anaconda environment, the following programs and package required to run all scripts
in this repo

- `pv` - progress viewer for opening text
- `psql` - PostgreSQL database for Drugcentral Data Dump parsing
- [hetnet-ml](https://github.com/mmayers12/hetnet-ml) - For feature extraction in machine learning pipeline.
    must be downloaded to the same parent directory as this repo

		$ cd ..
		$ git clone https://github.com/mmayers12/hetnet-ml.git


- [disease-ontology](https://github.com/mmayers12/disease-ontology) - For some ID cross references. Must be
    downloaded to the same parent directory as this repo

    	$ cd ..
    	$ git clone https://github.com/mmayers12/disease-ontology.git

- [UMLS Account](https://uts.nlm.nih.gov//license.html) - Required for downloading the UMLS Metathesaurs data.

