import os
import pickle
import pandas as pd
import seaborn as sns
from tqdm import tqdm
from collections import defaultdict

from hetnet_ml import graph_tools as gt

#### Condense Edge Semmantics
print('Running 03_Condense_edge_semantics.py')
print('Importing Nodes and Edges file')
nodes = gt.remove_colons(pd.read_csv('../data/nodes_VER43_R_nodes_consolidated.csv'))
edges = gt.remove_colons(pd.read_csv('../data/edges_VER43_R_nodes_consolidated.csv', converters={'pmids':eval}))
start_edge_num = len(edges)

def sanitize(x):
    """Some pmids have the appearance of '2015332 [3]' for some reason. This fixes that"""
    if type(x) == str:
        if ' ' in x:
            x = x.split(' ')[0]
    return x

# Some pmids are appearing as string, e.g. row 6.  They should all be int
edges['pmids'] = edges['pmids'].apply(lambda ids: set([int(sanitize(x)) for x in ids]))

edge_map = pd.read_csv('../data/edge_condense_map.csv')

def change_edge_type(from_type, to_type, swap=False):
    idx = edges.query('type == @from_type').index
    edges.loc[idx, 'type'] = to_type
    if swap:
        tmp = edges.loc[idx, 'start_id']
        edges.loc[idx, 'start_id'] = edges.loc[idx, 'end_id']
        edges.loc[idx, 'end_id'] = tmp
                                             
def merge_edge_types(from_list, to_type, swap=False):
    for from_type in from_list:
        change_edge_type(from_type, to_type, swap=swap)
        
def drop_edges_from_list(drop_edges):
    idx = edges.query('type in @drop_edges').index
    edges.drop(idx, inplace=True)
    
# Order is important here
# Previous iterations of this pipeline had multiple rounds of edge condensation
# so some edges will be changed multiple times, and going through the .csv in row order
# ensurse that these changes are all applied correctly.

print('Condensing edge semantics...')

for row in tqdm(edge_map.itertuples(), total=len(edge_map)):
    change_edge_type(row.original_edge, row.condensed_to, swap=row.reverse)
edges = edges.dropna(subset=['type']).reset_index(drop=True)

#### Remove duplicated undirected edges
print('Removing duplicated undirected edges after edge condensing')
abv, met = gt.get_abbrev_dict_and_edge_tuples(gt.add_colons(nodes), gt.add_colons(edges))
id_to_label = nodes.set_index('id')['label'].to_dict()
edges['start_label'] = edges['start_id'].map(lambda c: id_to_label[c])
edges['end_label'] = edges['end_id'].map(lambda c: id_to_label[c])
edges['sem'] = edges['type'].map(lambda e: '_'.join(e.split('_')[:-1]))

edges['abbrev'] = edges['type'].map(lambda e: e.split('_')[-1])

proper_abbrevs = []
for e in tqdm(edges.itertuples(), total=len(edges)):
    if '>' in e.abbrev:
        abbrev = abv[e.start_label] + abv[e.sem] + '>' + abv[e.end_label]
    else:
        abbrev = abv[e.start_label] + abv[e.sem] + abv[e.end_label]
    proper_abbrevs.append(abbrev)
    
edges['calc_abbrev'] = proper_abbrevs
idx = edges['calc_abbrev'] != edges['abbrev']


#### Undirected edges between two nodes of the same type should only have 1 instance
print('Remove duplicate edges between nodes of the same type...')
# Get the edges that are un-directed, between same type
idx = edges['start_label'] == edges['end_label']

self_refferential_types = edges.loc[idx, 'type'].unique()
self_refferential_types = [e for e in self_refferential_types if '>' not in e]

self_ref_idx = edges.query('type in @self_refferential_types').index
# Need to keep and combine the PMIDs for Node_1 -- Node_2 and Node_2 -- Node_1
# Get a map of this structure {edge_type: {edge_id: [pmids]}}

edge_map = {}

# Look at the self-reffrentual types
for kind in tqdm(self_refferential_types):
    
    # Need map from edges to pmids
    pmid_map = defaultdict(set)
    # Only look at 1 kind of edge
    subedges = edges.query('type == @kind')
    
    for row in subedges.itertuples():
        # Grab the edge ID, sorting, so lowest CUI first:
        #     If both 'C00001 -- C00002' and 'C00002 -- C00001' exist, effectively standarizes to 
        #     'C00001 -- C00002' while combining the PMID evidence
        edge_id = tuple(sorted([row.start_id, row.end_id]))
        
        # Store the pmids for that edge
        pmid_map[edge_id] = pmid_map[edge_id].union(row.pmids)
        # Keep all the mappings for edge type
        edge_map[kind] = pmid_map

        # Convert back to a DataFrame
kinds = []
start_ids = []
end_ids = []
pmids = []

for kind, e_dict in edge_map.items():
    # Restructure as lists for easy dataframe generation
    for (s_id, e_id), pms in e_dict.items():
        kinds.append(kind)
        start_ids.append(s_id)
        end_ids.append(e_id)
        pmids.append(pms)
        
fixed_edges = pd.DataFrame({'start_id': start_ids, 'end_id': end_ids, 'type': kinds, 'pmids': pmids})

print(f'Before De-duplication: {len(edges.loc[self_ref_idx]):,} Edges between nodes of the same type')
print(f'After De-duplication: {len(fixed_edges):,} Edges between nodes of the same type')
# Remove all the potential duplicated edges
print(f'Total Edges: {len(edges):,}')
edges.drop(self_ref_idx, inplace=True)
print(f'Edges between two different Metanodes or Directed Edges: {len(edges):,}')
# Then add back in all de-duplicated edgers
edges = pd.concat([edges, fixed_edges], sort=False)
print(f'Total edges with De-duped edges added back: {len(edges):,}')
edges = edges[['start_id', 'end_id', 'type', 'pmids']]


#### Finish de-duplication and merge any pmids between those duplicated edges
before_dedup = len(edges)

# Some edges now duplicated, de-duplicate and combine pmids
print('Remove duplicated edges and combine pmids... about 5 minutes')
grpd = edges.groupby(['start_id', 'end_id', 'type'])
edges = grpd['pmids'].apply(lambda Series: set.union(*Series.values)).reset_index()
print('Recounting the pmid numbers')
# re-count the pmid numbers
edges['n_pmids'] = edges['pmids'].apply(len)

after_dedup = len(edges)
print(f'Edges before final Deduplication: {before_dedup:,}')
print(f'Edges after final Deduplication: {after_dedup:,}')

print('Exporting files...')
# Sort values before writing to disk
nodes = nodes.sort_values('label')
edges = edges.sort_values('type')

# Add in colons required by neo4j
nodes = gt.add_colons(nodes)
edges = gt.add_colons(edges)

nodes.to_csv('../data/nodes_VER43_R_consolidated_condensed.csv', index=False)
edges.to_csv('../data/edges_VER43_R_consolidated_condensed.csv', index=False)

print('Complete processing 03_Condensing_edge_semantics.py')