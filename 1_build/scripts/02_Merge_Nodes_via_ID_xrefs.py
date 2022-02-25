# Code adapting 02-Merge_nodes_via_ID_xrefs jupyter notebook. 
# Meant as a script to download everything without going through the notebook

import os, pickle
from tqdm import tqdm

import numpy as np
import pandas as pd

from collections import defaultdict, Counter
from itertools import chain
from queue import Queue
import pickle 
import sys
sys.path.append('../tools')
from hetnet_ml import graph_tools as gt
import load_umls

#load data
print('Running 02_Merge_Nodes_via_ID_xrefs.py')
print('Importing DrugCentral information for Gold Standard and Add Compound Names')
print('Loading drugcentral_rel dataframe')
rels = pd.read_csv('../data/drugcentral_rel_20211005.csv')
print('Loading drugcentral_ids dataframe')
dc_ids = pd.read_csv('../data/drugcentral_ids_20211005.csv')
print('Loading drugcentral_syn dataframe')
syn = pd.read_csv('../data/drugcentral_syn_20211005.csv')
syn.rename(columns={'id': 'struct_id'}, inplace=True)
pref = syn.query('preferred_name == 1').reset_index(drop=True)
pref = pref.dropna(subset=['struct_id'])
pref['struct_id'] = pref['struct_id'].astype('int64')
struct_id_to_name = pref.set_index('struct_id')['name'].to_dict()
rels['c_name'] = rels['struct_id'].map(lambda i: struct_id_to_name.get(i, float('nan')))

#### Map compounds in Semmed DB to MeSH
print('Mapping compounds in semmed to MeSH')
nodes = gt.remove_colons(pd.read_csv('../data/nodes_VER43_R.csv'))
umls_to_mesh = pickle.load(open('../data/UMLS-CUI_to_MeSH-Descripctor.pkl', 'rb'))
umls_to_mesh_1t1 = {k: v[0] for k, v in umls_to_mesh.items() if len(v) == 1}
nodes['mesh_id'] = nodes['id'].map(lambda c: umls_to_mesh_1t1.get(c, float('nan')))
drugs = nodes.query('label == "Chemicals & Drugs"').copy()
print(f"{(drugs['mesh_id'].count() / drugs.shape[0]):.3%} of Drug IDs mapped via MeSH:")
print(f"{drugs['mesh_id'].count():,} of {drugs.shape[0]:,} Mapped to {drugs['mesh_id'].nunique():,}\ Unique MSH ids")

num_drugs = drugs['id'].nunique()
msh_compress_drugs = drugs['mesh_id'].fillna(drugs['id']).nunique()

print(f'{num_drugs - msh_compress_drugs/num_drugs:.3%} Reduction in Drugs by using MSH synonmyms\ {num_drugs:,} --> {msh_compress_drugs:,}')


#### Use UMLS MeSH mappings and Mappings from DrugCentral to ensure Maximum overlap
print('Generate MeSH to DrugCentral map')
dc_maps = dc_ids.query(f'id_type in {["MESH_DESCRIPTOR_UI", "MESH_SUPPLEMENTAL_RECORD_UI" , "UMLSCUI"]}')

drug_adj_list = defaultdict(set)

for row in tqdm(dc_maps.itertuples(), total=len(dc_maps)):
    drug_adj_list[row.struct_id].add(row.identifier)
    drug_adj_list[row.identifier].add(row.struct_id)
    
umls_keys = list(chain(*[[k]*len(v) for k, v in umls_to_mesh.items()]))
mesh_vals = list(chain(*[v for v in umls_to_mesh.values()]))
umls_to_mesh_df = pd.DataFrame({'umls': umls_keys, 'mesh': mesh_vals})
drug_ids = drugs['id'].unique()
umls_to_mesh_drugs = umls_to_mesh_df.query('umls in @drug_ids')
umls_set = set(drugs['id']) | set(dc_maps.query('id_type == "UMLSCUI"'))
mesh_set = set(mesh_vals) | set(dc_maps.query('id_type in {}'.format(["MESH_DESCRIPTOR_UI", "MESH_SUPPLEMENTAL_RECORD_UI"]))['identifier'])
for row in umls_to_mesh_drugs.itertuples():
    drug_adj_list[row.umls].add(row.mesh)
    drug_adj_list[row.mesh].add(row.umls)
# Ensure that all Struct IDs from DrugCentral make it into the subnets (even if no xrefs)
for struct_id in rels.query('relationship_name == "indication"')['struct_id'].unique():
    drug_adj_list[struct_id].add(struct_id)
def get_subnets(adj_list):

    all_identifiers = set(adj_list.keys())

    subnets = defaultdict(set)
    visited = set()

    for cui in tqdm(all_identifiers):
        if cui not in visited:
            visited.add(cui)
            q = Queue()
            q.put(cui)

            while not q.empty():
                cur = q.get()
                visited.add(cur)

                for neighbour in adj_list[cur]:
                    subnets[cui].add(neighbour)
                    if neighbour not in visited:
                        q.put(neighbour)
                        visited.add(neighbour)

    return subnets
subnets = get_subnets(drug_adj_list)
mesh_counts = umls_keys + mesh_vals + list(dc_maps['identifier']) + list(dc_maps['struct_id'].unique())
mesh_counts = Counter(mesh_counts)

rekeyed_subnets = dict()

for v in subnets.values():
    sort_sub = sorted(list(v), key=lambda k: (mesh_counts[k], k in mesh_set, k in umls_set), reverse=True)
    new_key = sort_sub[0]
    rekeyed_subnets[new_key] = v
# Final map is just inverse of the subnets dict
final_drug_map = dict()

for k, v in rekeyed_subnets.items():
    for val in v:
        final_drug_map[val] = k

print(f'Length of drug mappings {len(final_drug_map)}')
print('Exporting merge map')
pickle.dump(final_drug_map, open('../data/drug_merge_map.pkl', 'wb'))




#### 4. Map all the compounds and check
print('Mapping all the compounds to Drugs')
# Some items won't necessarily be mappable, so use original ID
drugs['new_id'] = drugs['id'].map(lambda i: final_drug_map.get(i, i))
# Map the Gold Standard indications as well
rels['compound_new_id'] = rels['struct_id'].map(lambda i: final_drug_map.get(i, i))
drugs['id_source'] = drugs['new_id'].map(lambda x: 'MeSH' if x in mesh_set else 'UMLS')
print(f'{(drugs.shape[0] - drugs["new_id"].nunique())/drugs.shape[0]:.3%} Reduction in Drugs {drugs.shape[0]:,} --> {drugs["new_id"].nunique():,}')

inds = rels.query('relationship_name == "indication"')
drug_ids_semmed = set(drugs['new_id'])
drugs_in_inds = set(inds['compound_new_id'].dropna())
num_ind_in_semmed = len(drugs_in_inds & drug_ids_semmed)

print(f'{(num_ind_in_semmed / len(drugs_in_inds)):.3%} of Drugs in DC Indications mapped: {num_ind_in_semmed:,} out of {len(drugs_in_inds):,}')
ind_semmed_comp = inds.query('compound_new_id in @drug_ids_semmed').shape[0]
print(f'{(ind_semmed_comp / len(inds)):.3%} of Indications have mappable Drug: {ind_semmed_comp:,} out of {len(inds):,}')




#### Diseases. Use DO Slim to try and get general diseases
print('Mapping diseases to Disease Ontology Slim IDs to generalize diseases')
diseases = nodes.query('label == "Disorders"').copy()
dis_numbers = diseases.groupby('mesh_id').apply(len).sort_values(ascending=False)
param = dis_numbers[:10].index.tolist()

conso = load_umls.open_mrconso()
snomed_xrefs = conso.query("SAB == 'SNOMEDCT_US'").dropna(subset=['CUI', 'SCUI'])
dis_adj_list = defaultdict(set)

disease_ids = set(diseases['id'].unique())
umls_to_mesh_dis = umls_to_mesh_df.query('umls in @disease_ids')

for row in umls_to_mesh_dis.itertuples():
    dis_adj_list[row.umls].add(row.mesh)
    dis_adj_list[row.mesh].add(row.umls)
    
# Convert the snomed concept ids to string since they're strings the adj_list
rels['snomed_conceptid'] = rels['snomed_conceptid'].map(lambda i: str(int(i)) if not pd.isnull(i) else i)

sub_rels = rels.dropna(subset=['snomed_conceptid', 'umls_cui'])

for row in sub_rels.itertuples():
    dis_adj_list[row.umls_cui].add(row.snomed_conceptid)
    dis_adj_list[row.snomed_conceptid].add(row.umls_cui)
    # Make sure to get mesh to CUI maps for the new cuis picked up via drugcentral
    if row.umls_cui in umls_to_mesh_1t1:
        dis_adj_list[umls_to_mesh_1t1[row.umls_cui]].add(row.umls_cui)
        dis_adj_list[row.umls_cui].add(umls_to_mesh_1t1[row.umls_cui])

ind_snomed = set(rels['snomed_conceptid'])
dis_umls = set(rels['umls_cui']) | disease_ids

dis_snomed_xrefs = snomed_xrefs.query('CUI in @dis_umls or SCUI in @ind_snomed')
print(f'Number of SNOMED-CUI terms mapped from DC to MESH: {len(dis_snomed_xrefs)}')

for row in tqdm(dis_snomed_xrefs.itertuples(), total=len(dis_snomed_xrefs)):
    dis_adj_list[row.CUI].add(row.SCUI)
    dis_adj_list[row.SCUI].add(row.CUI)
    # Make sure to get mesh to CUI maps for the new cuis picked up via drugcentral
    if row.CUI in umls_to_mesh_1t1:
        dis_adj_list[umls_to_mesh_1t1[row.CUI]].add(row.CUI)
        dis_adj_list[row.CUI].add(umls_to_mesh_1t1[row.CUI])

#### DO Slim Integration
print('Get DO Slim Ids from WikiData via WikiDataIntegrator')
from wikidataintegrator import wdi_core

query_text = """
select ?doid ?umlscui

WHERE
{
    ?s wdt:P699 ?doid .
    ?s wdt:P2892 ?umlscui .
}
"""

result = wdi_core.WDItemEngine.execute_sparql_query(query_text, as_dataframe=True)
result.to_csv('../data/doid-to-umls.csv', index=False)
doid_to_umls = result.set_index('doid')['umlscui'].to_dict()
slim_xref = pd.read_table('../../disease-ontology/data/xrefs-prop-slim.tsv')
do_slim = pd.read_table('../../disease-ontology/data/slim-terms-prop.tsv')

resources = ['SNOMEDCT_US_2016_03_01', 'UMLS', 'MESH', 'SNOMEDCT', 'SNOMEDCT_US_2015_03_01']
useful_xref = slim_xref.query('resource in @resources')
for row in useful_xref.itertuples():
    dis_adj_list[row.doid_code].add(row.resource_id)
    dis_adj_list[row.resource_id].add(row.doid_code)
    if row.resource == "UMLS" and row.resource_id in umls_to_mesh_1t1:
        dis_adj_list[umls_to_mesh_1t1[row.resource_id]].add(row.resource_id)
        dis_adj_list[row.resource_id].add(umls_to_mesh_1t1[row.resource_id])
do_slim['cui'] = do_slim['subsumed_id'].map(lambda d: doid_to_umls.get(d, float('nan')))
do_slim_d = do_slim.dropna(subset=['cui'])

for row in do_slim_d.itertuples():
    dis_adj_list[row.subsumed_id].add(row.cui)
    dis_adj_list[row.cui].add(row.subsumed_id)
    if row.cui in umls_to_mesh_1t1:
        dis_adj_list[umls_to_mesh_1t1[row.cui]].add(row.cui)
        dis_adj_list[row.cui].add(umls_to_mesh_1t1[row.cui])
        
do_slim_terms = do_slim.set_index('slim_id')['slim_name'].to_dict()
slim_ids = set(do_slim_terms.keys())

#### Make final map for Diseases and map them
print('Make final map for Diseases and map appropriate entities')
dis_subnets = get_subnets(dis_adj_list)
umls_set = set(diseases['id'].dropna()) | set(rels['umls_cui'].dropna())
umls_to_val = {u: 9999999-int(u[1:]) for u in umls_set}

mesh_counts = umls_keys + mesh_vals + list(rels['umls_cui'].map(lambda c: umls_to_mesh_1t1.get(c, c))) 
mesh_counts = Counter(mesh_counts)

rekeyed_dis_subnets = dict()

for v in dis_subnets.values():
    # If a disease was consolidated under DO-SLIM, take the slim ID and name
    if v & slim_ids:
        new_key = (v & slim_ids).pop()
        rekeyed_dis_subnets[new_key] = v
    else:
        # First take ones in the mesh, then by the highest number of things it consolidated
        # Then take the lowest numbered UMLS ID...
        sort_sub = sorted(list(v), key=lambda k: (k in mesh_set, mesh_counts[k], k in umls_set, umls_to_val.get(k, 0)), reverse=True)
        new_key = sort_sub[0]
        rekeyed_dis_subnets[new_key] = v
        
# Final map is just inverse of the subnets dict
final_dis_map = dict()

for k, v in rekeyed_dis_subnets.items():
    for val in v:
        final_dis_map[val] = k
        
diseases['new_id'] = diseases['id'].map(lambda i: final_dis_map.get(i, i))

# See how many instances of diseases mapped to 1 mesh ID had their ID changed through
# SNOMED and DO-SLIM consolidation
print(f"{diseases.dropna(subset=['mesh_id']).query('mesh_id != new_id')['id'].nunique():,} Original CUIs counts")
print(f"Mapped to {diseases.dropna(subset=['mesh_id']).query('mesh_id != new_id')['mesh_id'].nunique():,} MeSH IDs")
print(f"Consolidated to {diseases.dropna(subset=['mesh_id']).query('mesh_id != new_id')['new_id'].nunique():,} unique entities")

def dis_source_map(x):
    if x in mesh_set:
        return 'MeSH'
    elif x in umls_set:
        return 'UMLS'
    elif x.startswith('DOID:'):
        return 'DO-Slim'
    else:
        # Just in case there's a problem...
        return 'Uh-Oh'

diseases['id_source'] = diseases['new_id'].map(lambda x: dis_source_map(x))
pickle.dump(final_dis_map, open('../data/disease_merge_map.pkl', 'wb'))
print(f"{(diseases.shape[0] - diseases['new_id'].nunique())/diseases.shape[0]:.3%} Reduction in Diseases { diseases.shape[0]:,} --> {diseases['new_id'].nunique():,}")

rels['disease_new_id'] = rels['umls_cui'].map(lambda c: final_dis_map.get(c, c))
bad_idx = rels[rels['disease_new_id'].isnull()].index

rels.loc[bad_idx, 'disease_new_id'] = rels.loc[bad_idx, 'snomed_conceptid'].map(lambda c: final_dis_map.get(c, float('nan')))

inds = rels.query('relationship_name == "indication"')

disease_ids_semmed = set(diseases['new_id'])
diseases_in_inds = set(inds['disease_new_id'].dropna())

num_ind_in_semmed = len(diseases_in_inds & disease_ids_semmed)

print(f"{(num_ind_in_semmed / len(diseases_in_inds)):.3%} of diseases in DC Indications \
mapped: {num_ind_in_semmed:,} out of {len(diseases_in_inds):,}")

ind_semmed_comp = inds.query('disease_new_id in @disease_ids_semmed').shape[0]

print(f"{(ind_semmed_comp / len(inds)):.3%} of Indications have \
mappable disease: {ind_semmed_comp:,} out of {len(inds):,}")

inds_dd = inds.drop_duplicates(subset=['compound_new_id', 'disease_new_id'])

new_cids = set(drugs['new_id'].unique())
new_dids = set(diseases['new_id'].unique())
inds_in_semmed = inds_dd.query('compound_new_id in @new_cids and disease_new_id in @new_dids')
print(f"{len(inds_in_semmed) / len(inds_dd):.3%} of indications now have both compound and \
disease mappable {len(inds_in_semmed):,} out of {len(inds_dd):,}")


#### Add in Dates for indications ####
print('Get dates for drug approval indications')
app = pd.read_csv('../data/drugcentral_approvals_20211005.csv')
app = app.rename(columns={'approval': 'approval_date'})

app = (app.dropna(subset=['approval_date']) # Remove NaN values
          .sort_values('approval_date')     # Put the earliest approval_date first
          .groupby('struct_id')        # Group by the compound's id
          .first()                     # And select the first instance of that id
          .reset_index())              # Return struct_id to a column from the index
rels = pd.merge(rels, app[['struct_id', 'approval_date']], how='left', on='struct_id')
idx = rels[~rels['approval_date'].isnull()].index
rels.loc[idx, 'approval_year'] = rels.loc[idx, 'approval_date'].map(lambda s: s.split('-')[0])


#### Rebuild the Nodes to new ID mappings
print('Rebuilding nodes to appropriate new ID maps')
all_umls = set(nodes['id'])
umls_set = set(nodes['id']) | set(dc_maps.query('id_type == "UMLSCUI"')) |  set(rels['umls_cui'])
def get_source(cid):
    if cid in mesh_set:
        return 'MeSH'
    elif cid in umls_set:
        return 'UMLS'
    elif cid.startswith('DOID:'):
        return 'DO-Slim'
    else:
        return 'problem...'
pickle.dump(umls_set, open('../data/umls_id_set.pkl', 'wb'))
pickle.dump(mesh_set, open('../data/mesh_id_set.pkl', 'wb'))
new_nodes = nodes.query('label not in {}'.format(['Chemicals & Drugs', 'Disorders'])).copy()
new_nodes['new_id'] = new_nodes['mesh_id'].fillna(new_nodes['id'])
new_nodes['id_source'] = new_nodes['new_id'].apply(lambda c: get_source(c))

drug_dis = pd.concat([drugs, diseases])
curr_map = drug_dis.set_index('id')['new_id'].to_dict()
idx = drug_dis.groupby('new_id')['label'].nunique() > 1
problems = idx[idx].index.values

remap = dict()
grpd = drug_dis.query('new_id in @problems').groupby('new_id')

for grp, df in grpd:
    for labels in df['label'].unique():
        curr_label = df.query('label == @labels')['id'].values
        
        # Keep the MeSH Map for the New ID if its a Drug
        if labels == 'Chemcials & Drugs':
            for c in curr_label:
                remap[c] = grp
                
        # Use a random Disease CUI if its a Disease
        else:
            new_cui = curr_label[0]
            for c in curr_label:
                remap[c] = new_cui

drug_dis['new_id'] = drug_dis['id'].map(lambda i: remap.get(i, curr_map[i]))
new_nodes = pd.concat([new_nodes, drug_dis])
new_nodes = new_nodes.sort_values('label')
idx = new_nodes.groupby('new_id')['label'].nunique() > 1
problems = idx[idx].index.values

#### fix other node-type conflicts
edges = gt.remove_colons(pd.read_csv('../data/edges_VER43_R.csv', converters={'pmids':eval}))
cui_counts = edges['start_id'].value_counts().add(edges['end_id'].value_counts(), fill_value=0).to_dict()
# For now, just return conflicting nodes to thier old semmantic type
grpd = new_nodes.query('new_id in @problems').groupby('new_id')
remap = dict()

for msh_id, df in tqdm(grpd, total=len(grpd)):

    # Get all the labels and counts for those labels
    labels = df['label'].unique().tolist()
    counts = df['label'].value_counts().to_dict()
    
    # Sort the by the Number of different nodes mapped to that label
    labels = sorted(labels, key=lambda l: counts[l], reverse=True)
    
    # Chemicals and Drugs and Diseases have higher priorities in the context of machine learning
    # So any item that could be either of those types will be set to them automatically.
    drug_or_dis = False
    
    # Select the Chemicals & Drugs nodes to have the MeSH ID if possible
    if 'Chemicals & Drugs' in labels:
        labels.remove('Chemicals & Drugs')
        curr_label = df.query('label == "Chemicals & Drugs"')['id'].values
        drug_or_dis = True
        for c in curr_label:
            remap[c] = msh_id    
    
    # Otherwise, elect the Disorders nodes to have the MeSH ID if possible
    elif 'Disorders' in labels:
        labels.remove('Disorders')
        curr_label = df.query('label == "Disorders"')['id'].values
        drug_or_dis = True
        for c in curr_label:
            remap[c] = msh_id    
    
    # Finally assign a merged CUI based on edge counts
    for i, label in enumerate(labels):
        curr_label = df.query('label == @label')['id'].values
        
        # Give highest counts of nodes the MeSH ID, if not already assigned to a Drug or Disease
        if i == 0 and not drug_or_dis:
            new_cui = msh_id
        else:
            # For types that won't get a MeSH ID, 
            # get the CUI that has largest number of instances in the edges
            new_cui = sorted(curr_label, key=lambda v: cui_counts.get(v, 0), reverse=True)[0]
        for c in curr_label:
            remap[c] = new_cui
            
# Perform the new Mapping
curr_map = new_nodes.set_index('id')['new_id'].to_dict()
new_nodes['new_id'] = nodes['id'].map(lambda i: remap.get(i, curr_map[i]))

# Ensure there are now no problems
idx = new_nodes.groupby('new_id')['label'].nunique() > 1
problems = idx[idx].index.values
print(len(problems))

num_old_ids = new_nodes['id'].nunique()
num_new_ids = new_nodes['new_id'].nunique()

print(f"{(num_old_ids-num_new_ids)/num_old_ids:.3%} reduction \
in the number of NODES\n{num_old_ids:,} --> {num_new_ids:,}")
new_nodes['id_source'] = new_nodes['new_id'].apply(lambda c: get_source(c))

cui_to_name = nodes.set_index('id')['name'].to_dict()
cui_to_name = {**cui_to_name, **rels.set_index('umls_cui')['concept_name'].to_dict()}
cui_to_name = {**cui_to_name, **rels.set_index('compound_new_id')['c_name'].to_dict()}

msh_to_name = pickle.load(open('../data/MeSH_DescUID_to_Name.pkl', 'rb'))
# The mappings from UMLS are less reliable, so use the ones that came from MeSH itself first
msh_to_name = {**pickle.load(open('../data/MeSH_id_to_name_via_UMLS.pkl', 'rb')), **msh_to_name}

id_to_name = {**struct_id_to_name, **do_slim_terms, **cui_to_name, **msh_to_name}
new_nodes['name'] = new_nodes['new_id'].map(lambda i: id_to_name[i])
pickle.dump(id_to_name, open('../data/all_ids_to_names.pkl', 'wb'))
final_node_map = new_nodes.set_index('id')['new_id'].to_dict()


#### Map all the edges remaining
print('Map re-mapped nodes to the edges')
edges['start_id'] = edges['start_id'].map(lambda c: final_node_map[c])
edges['end_id'] = edges['end_id'].map(lambda c: final_node_map[c])

print('Mapping Node Ids to the edges. Duplicated edges will be deleted. \
This can take up to 30 minutes')
num_before = len(edges)
# Some edges now duplicated, de-duplicate and combine pmids
grpd = edges.groupby(['start_id', 'end_id', 'type'])
edges = grpd['pmids'].apply(lambda Series: set.union(*Series.values)).reset_index()
# re-count the pmid numbers
edges['n_pmids'] = edges['pmids'].apply(len)
num_after = len(edges)

print(f"There are: {num_before:,} Edges before node consolidation")
print(f"There are: {num_after:,} Edges after node consolidation")
print(f"A {((num_before - num_after) / num_before):.3%} reduction in edges")

#### Save files to the network
print('Saving Network files (consolidated nodes and edges)')
# Get rid of the old ids in the nodes
new_nodes.drop('id', axis=1, inplace=True)
new_nodes = new_nodes.rename(columns={'new_id': 'id'})[['id', 'name', 'label', 'id_source']]
new_nodes = new_nodes.drop_duplicates(subset='id')

# Sort values before writing to disk
new_nodes = new_nodes.sort_values('label')
edges = edges.sort_values('type')

# Add in colons required by neo4j
new_nodes = gt.add_colons(new_nodes)
edges = gt.add_colons(edges)

new_nodes.to_csv('../data/nodes_VER43_R_nodes_consolidated.csv', index=False)
edges.to_csv('../data/edges_VER43_R_nodes_consolidated.csv', index=False)
pickle.dump(final_node_map, open('../data/node_id_merge_map.pkl', 'wb'))

#### Save relationship files for ML gold standard
print('Saving a seperate "indications" file for a Machine Learning gold standard')
# Do some rennaming of the columns before saving
rels = rels.rename(columns={'c_name': 'compound_name',
                            'concept_name': 'disease_name',
                            'compound_new_id': 'compound_semmed_id',
                            'disease_new_id': 'disease_semmed_id'})
# Only want indications for the gold standard
# Keep Duplicates in RELs just in case they're insightful, but indicaitons should have no dups.
inds = rels.query('relationship_name == "indication"').drop_duplicates(subset=['compound_semmed_id', 'disease_semmed_id'])

rels.to_csv('../data/gold_standard_relationships_nodemerge.csv', index=False)
inds.to_csv('../data/indications_nodemerge.csv', index=False)

print('Complete processing 02_Merge_Nodes_via_ID_xrefs.py')