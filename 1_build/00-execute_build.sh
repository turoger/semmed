#!/bin/bash

python ./scripts/02_Merge_Nodes_via_ID_xrefs.py --dc_date '20220822' --semmed_version 'VER43_R'
python ./scripts/03_Condense_edge_semmantics.py --semmed_version 'VER43_R'
python ./scripts/04_filter_low_abundance_edges.py --semmed_version 'VER43_R'
python ./scripts/05_Keep_Six_relevant_metanodes.py --semmed_version 'VER43_R'
python ./scripts/06_Resolve_Network_Edges_by_Time.py --semmed_version 'VER43_R' --base_dir '../data/time_networks-6_metanode'