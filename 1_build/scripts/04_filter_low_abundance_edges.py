import os
from hetnet_ml import graph_tools as gt
import pickle

import pandas as pd
import numpy as np
from tqdm import tqdm

print(f'Running 04_filter_low_abundance_edges.py')
#### Filter low abundance edges
print('Filtering low abundance edges')
print('Loading data')
nodes = pd.read_csv('../data/nodes_VER43_R_consolidated_condensed.csv')
edges = pd.read_csv('../data/edges_VER43_R_consolidated_condensed.csv')

# remove : character from column names to make them queryable
nodes = gt.remove_colons(nodes)
edges = gt.remove_colons(edges)

comps = (nodes['label'] == 'Chemicals & Drugs').sum() 
diseases = (nodes['label'] == 'Disorders').sum()

print('Total combination of Compound to Disease Pairs')
print(f'{comps:,} Compounds x {diseases:,} Diseases = {comps*diseases:,} C-D pairs')
counts = edges["type"].value_counts()
      
print(f'There are {edges.shape[0]:,} total edges')
cutoff = 0.001
print(f'setting the cutoff to: {cutoff:3f}')
print(f"Number of edge types with this cutoff: {(counts > cutoff*len(edges)).sum()}")
print(f"Number of edges that remain with this cutoff: {counts[counts > cutoff*len(edges)].sum():,}")
      
ok_edges = list(counts[counts > cutoff*len(edges)].index)
result = edges.query('type in @ok_edges')


edge_ids = set(result['start_id'].unique()).union(set(result['end_id'].unique()))
node_ids = set(nodes['id'].unique())
not_in_edges = node_ids - edge_ids
not_in_edges = list(not_in_edges)
idx = nodes.query('id in @not_in_edges').index
nodes = nodes.drop(idx)

#### remove self refferential edges 
print('Calculating node degrees')
combo = gt.combine_nodes_and_edges(gt.add_colons(nodes), gt.add_colons(result[['start_id', 'end_id', 'type', 'n_pmids']]))
combo = gt.remove_colons(combo)
max_edge = combo["n_pmids"].max()
result = gt.remove_colons(result)
idx = result.query('start_id == end_id').index
result = result.drop(idx)
node_degrees = pd.concat([combo['start_name'], combo['end_name']]).value_counts()
      
#These are from the 100 most common nodes, removing things that are too general to be useful

too_general = ['Patients', 'Cells', 'Syndrome', 'Therapeutics', 'Proteins', 'Pharmaceutical Preparations',
 'Mice', 'Human', 'Child', 'Genes', 'Woman', 'Tissues', 'Growth', 'Individual', 'Antibodies',
 'Surgical Procedures, Operative', 'Adult', 'Enzymes', 'Symptoms', 'Animals', 'Cell Line', 'Wounds and Injuries', 'Complication',
 'House mice', 'Functional disorder', 'Infant', 'Family', 'Persons', 'Male population group', 'Monoclonal Antibodies',
 'Toxic effect', 'Infection', 'DNA', 'Control Groups', 'Injection procedure', 'Ions', 'Transcription, Genetic',
 'Organ', 'TRANSCRIPTION FACTOR', 'cohort']
idx = nodes.query('name in @too_general').index
nodes = nodes.drop(idx).reset_index(drop=True)
node_ids = nodes['id'].unique()
result = result.query('start_id in @node_ids and end_id in @node_ids')
print(f'Remove general nodes from the edges. {len(result):,} Edges remain')

counts = (result[result['n_pmids'] < 21]['n_pmids'].value_counts()
                                                   .rename('counts')
                                                   .to_frame()
                                                   .reset_index()
                                                   .rename(columns={'index':'n_pmids'}))
for i in range(1, 11):
    print(f"Edges with at least {i} unique PMIDs: {(result['n_pmids'] >= i).sum():,}")

print('Printing low-abundance filtered output')
result = gt.add_colons(result)
nodes = gt.add_colons(nodes)

result.to_csv('../data/edges_VER43_R_consolidated_condensed_filtered_001.csv', index=False)
nodes.to_csv('../data/nodes_VER43_R_consolidated_condensed_filtered_001.csv', index=False)
print('Complete processing 04_filter_low_abundance_edges.py')
