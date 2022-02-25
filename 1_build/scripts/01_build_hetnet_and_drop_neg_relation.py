# Code adapting 01-building_the_hetnet jupyter notebook. 
# Meant as a script to download everything without going through the notebook
# This script will generate a dataset that removes all "NEG_" edges
import os, pickle
from tqdm import tqdm

import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from collections import defaultdict
import sys
sys.path.append('../tools')
from hetnet_ml import graph_tools as gt
import load_umls

print(f'Running 01_build_hetnet_and_remove_neg_edges.py')
#### Get Unique PMID to (Sub, Pred, Obj) ####
print(f'Reading in Dataframes...')
# read dataframe
sem_df = pd.read_csv('../data/semmedVER43_R_clean_de-depricate.csv')
print(f'{sem_df.shape[0]:,} edges were imported')
print(f'Removing all nodes with novelties that are 0')
sem_df = sem_df.query('SUBJECT_NOVELTY != 0 and OBJECT_NOVELTY != 0')
print(f'{sem_df.shape[0]:,} edges remain after removing non-novel nodes')

# append triples to a dictionary
print('Generating a PMID to triple dictionary...')
pmids = defaultdict(set)
col_names = sem_df.columns.tolist()

pmid_idx = col_names.index('PMID')
sub_idx = col_names.index('SUBJECT_CUI')
pred_idx = col_names.index('PREDICATE')
obj_idx = col_names.index('OBJECT_CUI')

for tup in tqdm(sem_df.itertuples(False, None), total=len(sem_df)):
    
    pmid = tup[pmid_idx]
    sub = tup[sub_idx]
    pred = tup[pred_idx]
    obj = tup[obj_idx]
    
    pmids[(sub, pred, obj)].add(pmid)

# Get count stats
counts = [len(v) for v in pmids.values()]
counts = (pd.Series(counts)
            .value_counts()
            .to_frame()
            .reset_index()
            .rename(columns={'index': 'Num PMIDs', 0: 'SPO Triples'}))
counts['log Triples'] = np.log(counts['SPO Triples'])
for i in range(1, 11):
    print(f"Edges with at least {i:,} unique PMIDs: {counts[counts['Num PMIDs'] >= i]['SPO Triples'].sum():,}")
    

#### Prepare Node File ####
# de-duplicate the semmed dataframe
print(f'Raw Rows prior to duplicate removal in dataframe: {sem_df.shape[0]:,}')
sem_df = sem_df.drop_duplicates(subset=['SUBJECT_CUI', 'SUBJECT_NAME', 'SUBJECT_SEMTYPE', 'PREDICATE',
                                        'OBJECT_CUI', 'OBJECT_NAME', 'OBJECT_SEMTYPE'])
print(f'Rows remaining after duplicate removal in dataframe: {sem_df.shape[0]:,}')

# load semantic abbreviation types
abbv_to_type = dict()
with open('../data/SemanticTypes_2018AB.txt') as fin:
    for line in fin:
        line = line.strip()
        lspt = line.split('|')
        abbv_to_type[lspt[0]] = lspt[-1]
type_to_abbv = {v:k for k, v in abbv_to_type.items()}

# load semantic super types
abbv_to_super = dict()
with open('../data/SemGroups_2018.txt') as fin:
    for line in fin:
        line = line.strip()
        lspt = line.split('|')
        abbv_to_super[type_to_abbv[lspt[-1]]] = lspt[1]
        
# Fix semtype abbreviations in SemmedDB
semtype_abbvs = abbv_to_super.keys()
unused_abbvs = set(sem_df.query('SUBJECT_SEMTYPE not in @semtype_abbvs')['SUBJECT_SEMTYPE'].unique())
unused_abbvs.update(set(sem_df.query('OBJECT_SEMTYPE not in @semtype_abbvs')['OBJECT_SEMTYPE'].unique()))
# Looked ups a few cuis with these and they are all of the living beings type, 
# so we will set these to that semtype
abbv_to_super['invt'] = 'Living Beings'
abbv_to_super['alga'] = 'Living Beings'
abbv_to_super['rich'] = 'Living Beings'

# a few of them are chemicals & Drugs
abbv_to_super['carb'] = 'Chemicals & Drugs'
abbv_to_super['lipd'] = 'Chemicals & Drugs'
abbv_to_super['nsba'] = 'Chemicals & Drugs'
abbv_to_super['strd'] = 'Chemicals & Drugs'

# Get the nodes ready to be in a CSV using Neo4j import format.
# column headers ID (unique ID), name (str name), and Label (node type).
snodes = pd.DataFrame()
onodes = pd.DataFrame()


snodes['ID'] = sem_df['SUBJECT_CUI']
snodes['name'] = sem_df['SUBJECT_NAME']
snodes['label'] = sem_df['SUBJECT_SEMTYPE'].apply(lambda x: abbv_to_super.get(x))

onodes['ID'] = sem_df['OBJECT_CUI']
onodes['name'] = sem_df['OBJECT_NAME']
onodes['label'] = sem_df['OBJECT_SEMTYPE'].apply(lambda x: abbv_to_super.get(x))


nodes = pd.concat([snodes, onodes])
nodes.drop_duplicates(inplace=True)
nodes = nodes.reset_index(drop=True)

print(f"There are {nodes.shape[0]:,} entries and {nodes['ID'].nunique():,} unique IDs")
num_duped = nodes[nodes.duplicated(subset='ID', keep=False)].drop_duplicates().shape[0]
print(f'{num_duped:,} IDs have been found to have multiple semantic types')

# check for untyped nodes
no_semtype = nodes.isnull().sum(axis=1).astype(bool)

#### fix untyped nodes using UMLS Metathesaurus
mrsty = load_umls.open_mrsty()
cui_to_t_code = defaultdict(list)

col_names = mrsty.columns.tolist()
cui_idx = col_names.index('CUI')
t_idx = col_names.index('TUI')

for line in tqdm(mrsty.itertuples(index=False, name=None), total=len(mrsty)):
    cui_to_t_code[line[cui_idx]].append(line[t_idx])

# get map from t-code to super semmantic type
t_code_to_super = dict()
with open('../data/SemGroups_2018.txt') as fin:
    for line in fin:
        line = line.strip()
        ls = line.split('|')
        t_code_to_super[ls[2]] = ls[1]

# get cui_to_super dictionary and calculate number of issues    
cui_to_super = dict()
issues = dict()

for k, v in cui_to_t_code.items():
    # Only 1 T-Code, so map to its super semmantic type
    if len(v) == 1:
        cui_to_super[k] = t_code_to_super[v[0]]
    else:
        # Most multiple T-Codes should have the same supertype
        v_set = set([t_code_to_super[x] for x in v])
        if len(v_set) == 1:
            cui_to_super[k] = v_set.pop()
        # If they don't have the same supertype, return as a list...
        else:
            # If nothing prints they we have true 1-1 mapping and noting to worry about
            issues[k] = v_set
            
if issues:
    print(f'There are {len(issues):,} CUIs with multiple Supertypes...') 
    
# See what kind of semtypes pair together... Can we make some kind of choice to overcome conflict?
multi_sems = sorted(list(set([' | '.join(sorted(list(v))) for v in issues.values()])))
def examine_problem_cuis(combined):
    type_1, type_2 = combined.split(' | ')
    
    problems = []

    for k, v in issues.items():
        if type_1 in v and type_2 in v:
            problems.append(k)
        
    return nodes.query('ID in @problems')[['ID', 'name']].drop_duplicates()
# Make a function to fix the mapings for these IDs
def set_semtype(problems, new_sem):
    res = examine_problem_cuis(problems).copy()
    res['type'] = new_sem
    return res.set_index('ID')['type'].to_dict()

fixed_sems = {}
idx = 0
examine_problem_cuis(multi_sems[idx])
fixed_sems.update(set_semtype(multi_sems[idx], 'Activities & Behaviors'))
idx = 1
examine_problem_cuis(multi_sems[idx])
fixed_sems.update(set_semtype(multi_sems[idx], 'Anatomy'))
idx = 3
examine_problem_cuis(multi_sems[idx])
fixed_sems.update(set_semtype(multi_sems[idx], 'Objects'))
idx = 4
examine_problem_cuis(multi_sems[idx])
fixed_sems.update(set_semtype(multi_sems[idx], 'Phenomena'))
idx = 5
examine_problem_cuis(multi_sems[idx])
fixed_sems.update(set_semtype(multi_sems[idx], 'Concepts & Ideas'))
idx = 6
examine_problem_cuis(multi_sems[idx])
fixed_sems.update(set_semtype(multi_sems[idx], 'Concepts & Ideas'))
idx = 7
examine_problem_cuis(multi_sems[idx]).sample(20)
fixed_sems.update(set_semtype(multi_sems[idx], 'Organizations'))
cui_to_super.update(fixed_sems)
before_len = len(nodes)
nodes['label'] = nodes['ID'].apply(lambda x: cui_to_super[x])
nodes = nodes[['ID', 'name', 'label']].drop_duplicates().reset_index(drop=True)
print(f"Re-mapped {before_len-len(nodes):,} rows.\nWent from {before_len:,} rows in nodes to {len(nodes):,} with {nodes['ID'].nunique():,} unique CUIS.")
    
# remove spurious predicates
sem_df = sem_df.query('PREDICATE!="1532"').query('PREDICATE!="PREP"')

# get relations. There are 63 here
p_abv = {
     'ADMINISTERED_TO': 'at',
     'AFFECTS': 'af',
     'ASSOCIATED_WITH': 'aw',
     'AUGMENTS': 'ag',
     'CAUSES': 'c',
     'COEXISTS_WITH': 'cw',
     'COMPLICATES': 'cp',
     'CONVERTS_TO': 'ct',
     'DIAGNOSES': 'dg',
     'DISRUPTS': 'ds',
     'INHIBITS': 'in',
     'INTERACTS_WITH': 'iw',
     'ISA': 'i',
     'LOCATION_OF': 'lo',
     'MANIFESTATION_OF': 'mfo',
     'MEASUREMENT_OF': 'mso',       # new
     'MEASURES': 'ms',              # new
     'METHOD_OF': 'mo',
     'NEG_ADMINISTERED_TO': 'nat',
     'NEG_AFFECTS': 'naf',
     'NEG_ASSOCIATED_WITH': 'naw',
     'NEG_AUGMENTS': 'nag',
     'NEG_CAUSES': 'nc',
     'NEG_COEXISTS_WITH': 'ncw',
     'NEG_COMPLICATES': 'ncp',
     'NEG_CONVERTS_TO': 'nct',
     'NEG_DIAGNOSES': 'ndg',
     'NEG_DISRUPTS': 'nds',
     'NEG_INHIBITS': 'nin',
     'NEG_INTERACTS_WITH': 'niw',
     'NEG_ISA': 'ni',              # new
     'NEG_LOCATION_OF': 'nlo',
     'NEG_MANIFESTATION_OF': 'nmfo',
     'NEG_MEASUREMENT_OF': 'nmso', # new
     'NEG_MEASURES': 'nms',        # new
     'NEG_METHOD_OF': 'nmo',
     'NEG_OCCURS_IN': 'noi',
     'NEG_PART_OF': 'npo',
     'NEG_PRECEDES': 'npc',
     'NEG_PREDISPOSES': 'nps',
     'NEG_PREVENTS': 'npv',
     'NEG_PROCESS_OF': 'npro',
     'NEG_PRODUCES': 'npd',
     'NEG_STIMULATES': 'nst',
     'NEG_TREATS': 'nt',
     'NEG_USES': 'nu',
     'NEG_higher_than': 'nht',
     'NEG_lower_than': 'nlt',
     'NEG_same_as': 'nsa',         # new
     'OCCURS_IN': 'oi',
     'PART_OF': 'po',
     'PRECEDES': 'pc',
     'PREDISPOSES': 'ps',
     'PREVENTS': 'pv',
     'PROCESS_OF': 'pro',
     'PRODUCES': 'pd',
     'STIMULATES': 'st',
     'TREATS': 't',
     'USES': 'u',
     'compared_with': 'cpw',
     'higher_than': 'df',
     'lower_than': 'lt',
     'same_as': 'sa'
}

# get node abbreviations
sem_abv = {
 'Activities & Behaviors': 'AB',
 'Anatomy': 'A',
 'Compound': 'C', # don't exist in new build
 'Chemicals & Drugs': 'CD',
 'Concepts & Ideas': 'CI',
 'Devices': 'DV',
 'Disease': 'D', # don't exist in new build
 'Disorders': 'DO',
 'Genes & Molecular Sequences': 'G',
 'Geographic Areas': 'GA',
 'Living Beings': 'LB',
 'Objects': 'OB',
 'Occupations': 'OC',
 'Organizations': 'OR',
 'Phenomena': 'PH',
 'Physiology': 'PS',
 'Procedures': 'PR'
}

# Edge directions. There are 32 here.
edge_dir = {
     'ADMINISTERED_TO': '',
     'AFFECTS': '',
     'ASSOCIATED_WITH': '',
     'AUGMENTS': '',
     'CAUSES': '',
     'COEXISTS_WITH': '',
     'COMPLICATES': '',
     'CONVERTS_TO': '>',
     'DIAGNOSES': '',
     'DISRUPTS': '',
     'INHIBITS': '',
     'INTERACTS_WITH': '',
     'ISA': '>',
     'LOCATION_OF': '',
     'MANIFESTATION_OF': '>',
     'MEASUREMENT_OF': '',       # new.
     'MEASURES': '',              # new.
     'METHOD_OF': '',    
     'OCCURS_IN': '',
     'PART_OF': '>',
     'PRECEDES': '>',
     'PREDISPOSES': '',
     'PREVENTS': '',
     'PROCESS_OF': '>',
     'PRODUCES': '>',
     'STIMULATES': '',
     'TREATS': '',
     'USES': '',
     'compared_with': '',
     'higher_than': '',
     'lower_than': '',
     'same_as': ''
}
# Make a mapper from SEMTYPE to sem abbrev
kind_map = {}

for k, v in abbv_to_super.items():
    kind_map[k] = sem_abv[v]
id_to_type = nodes.set_index('ID')['label'].to_dict()
id_to_type_abbv = {k: sem_abv.get(v) for k, v in id_to_type.items()}

edges = pd.DataFrame()
edges['start_id'] = sem_df['SUBJECT_CUI']
edges['end_id'] = sem_df['OBJECT_CUI']
edges['type'] = sem_df['PREDICATE']
edge_store = len(edges)
edges = edges.query('type in @p_abv.keys()')
print(f'Checking for corrupted edges.\nThere are {len(edges)-len(edges):,} corrupted edges')

#### Removing negative types
edges['Neg'] = edges['type'].apply(lambda x: 1 if x[0:4]=="NEG_" else 0)
# seperate by column
edges = edges.query('Neg==0')
print('Removing Negative relations')


#### Putting together Edge Abbreviations
# figure out which edges need start and ends swapped
# add in pmids supporting each edge
# do the ID swap

# create abbreviated meta edges
edges['abbrev'] = (edges['start_id'].apply(lambda x: id_to_type_abbv.get(x,x)) 
                    + edges['type'].apply(lambda x: p_abv.get(x,x))
                    + edges['type'].apply(lambda x: edge_dir.get(x,''))
                    + edges['end_id'].apply(lambda x: id_to_type_abbv.get(x,x)))

edges['rev_abbrev'] = (edges['end_id'].apply(lambda x: id_to_type_abbv.get(x,x)) 
                    + edges['type'].apply(lambda x: edge_dir.get(x,''))
                    + edges['type'].apply(lambda x: p_abv.get(x,x))
                    + edges['start_id'].apply(lambda x: id_to_type_abbv.get(x,x)))
edge_store = len(edges)
edges = edges.drop_duplicates().drop(columns='Neg')
print(f'Number of edges {edge_store:,}\nNumber of edges after dropping duplicates {len(edges):,}')
def validate_abbrevs(e_type, abbrev, rev_abbrev):
    if rev_abbrev not in abbreviations:
        abbreviations.update([abbrev])
        return str(e_type) + '_' + str(abbrev)
    else:
        return str(e_type) + '_' + str(rev_abbrev)
    
abbreviations = set()
new_types = []
for row in tqdm(edges.itertuples(), total=len(edges)):
    new_types.append(validate_abbrevs(row[3], row[4], row[5]))
edges = edges.reset_index(drop=True)
# add in PMID counts as extra data for each edge
edges['pmids'] = pd.Series([pmids[i] for i in tqdm(edges.set_index(['start_id', 'type', 'end_id']).index.tolist())])

#swap start and end IDs for edges where start and end type were swapped
e_types_current = edges['type'] + '_' + edges['abbrev']
need_swap = e_types_current != pd.Series(new_types)
tmp = edges.loc[need_swap, 'start_id']
edges.loc[need_swap, 'start_id'] = edges.loc[need_swap, 'end_id']
edges.loc[need_swap, 'end_id'] = tmp
edges['type'] = new_types
# drop extra columns
edges = edges[['start_id', 'end_id', 'type', 'pmids']]

# deduplicate edges as some edges are now duplicated.
print('Deduplicating edges. This takes approximately 6 - 30 minutes depending on your computer')
grpd = edges.groupby(['start_id', 'end_id', 'type'])
edges = grpd['pmids'].apply(lambda Series: set.union(*Series.values)).reset_index()
edges['n_pmids'] = edges['pmids'].apply(len)
nodes = nodes.dropna()

# ensure all ids in the edges appear in the nodes
edge_ids = set(edges['start_id']).union(set(edges['end_id']))

# remove nodes that don't appear in the edges
edge_ids = list(edge_ids)
nodes = nodes.query('ID in @edge_ids')
node_ids = set(nodes['ID'])

print(f'There are {len(edge_ids):,} unique edge set node ids')
print(f'There are {len(node_ids):,} unique node ids')
print(f'Edge Node and Node Set difference is {len(edge_ids)-len(node_ids):,}. (0 expected.)')

print(f'Number of Nodes: {len(nodes):,}')
print(f'Number of Edges: {len(edges):,}')

print(f'Saving Nodes and Edges data to: {os.getcwd()[0:-8]+"/data/"}')
# Sort values before writing to disk
nodes = nodes.sort_values('label')
edges = edges.sort_values('type')

# Add in colons required by neo4j
nodes = gt.add_colons(nodes)
edges = gt.add_colons(edges)

nodes.to_csv('../data/nodes_VER43_R.csv', index=False)
edges.to_csv('../data/edges_VER43_R.csv', index=False)

print('Complete processing "build_hetnet_and_remove_neg_edges.py"')