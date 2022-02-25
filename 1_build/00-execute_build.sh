#!/bin/bash

python ./scripts/02_Merge_Nodes_via_ID_xrefs.py
python ./scripts/03_Condense_edge_semmantics.py
python ./scripts/04_filter_low_abundance_edges.py
python ./scripts/05_Keep_Six_relevant_metanodes.py
python ./scripts/06_Resolve_Network_Edges_by_Time.py