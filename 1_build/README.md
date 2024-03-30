# Build

This folder builds the heterogenous time-resolved knowledge graph from the downloaded content in `../0_prepare`. Please run the scripts there before running these.

## To run

Run the `./building.py` script with appropriate flags.  Approximate runtime for all notebooks is ~1 hour.
```
conda run -n mini_semmed --no-capture-output python building.py # non-interactive

python building.py -v [semmed version] -d [drugcentral date] -y \  
[hyperparameter optimization year] --split_hyperparameter_optimization \
--include_time --split_train_test_valid --drop_negative_relations \
-- convert_negative_relations 
```

