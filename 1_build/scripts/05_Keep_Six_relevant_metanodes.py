import pandas as pd
from hetnet_ml import graph_tools as gt
from hetnetpy.hetnet import MetaGraph

print(f'Running 05_Keep_Six_relevant_metanodes.py')
print(f'Importing nodes and edges')
nodes = gt.remove_colons(pd.read_csv('../data/nodes_VER43_R_consolidated_condensed_filtered_001.csv'))
edges = gt.remove_colons(pd.read_csv('../data/edges_VER43_R_consolidated_condensed_filtered_001.csv'))

print(f'Getting abbreviations and metapaths')
abv, met = gt.get_abbrev_dict_and_edge_tuples(gt.add_colons(nodes), gt.add_colons(edges))

print(f'Remove duplicated edges that are duplicated when "reversed"')
met.remove(('Disorders','Physiology','AFFECTS','both'))
met.remove(('Disorders','Physiology','CAUSES','both'))

print(f'Get metagraph')
mg = MetaGraph.from_edge_tuples(met,abv)

def num_metapaths(nodes, edges):
    abv, met = gt.get_abbrev_dict_and_edge_tuples(gt.add_colons(nodes), gt.add_colons(edges))
    try:
        met.remove(('Disorders','Physiology','AFFECTS','both'))
        met.remove(('Disorders','Physiology','CAUSES','both'))
    except:
        pass
    return len(MetaGraph.from_edge_tuples(met, abv).extract_metapaths('Chemicals & Drugs', 'Disorders', 4))

print(f'{num_metapaths(nodes, edges):,} Potnential Metapaths')


#### Removing un-needed metanodes
print(f'Removing un-needed metanodes')
remove_types = ['Organizations', 'Activities & Behaviors', 'Concepts & Ideas', 'Procedures', 'Devices', 'Living Beings']
idx = gt.remove_colons(nodes).query('label in @remove_types').index
nodes.drop(idx, inplace=True)

ok_ids = nodes['id'].unique()
edges = edges.query('start_id in @ok_ids and end_id in @ok_ids')

print(f'{len(ok_ids):,} Unique IDs in the nodes')
print(f"{len(set(edges['start_id']).union(set(edges['end_id']))):,} Unique IDs found within the remaining edges")

ok_ids = list(set(edges['start_id']).union(set(edges['end_id'])))
nodes = nodes.query('id in @ok_ids')

print('Remvoed IDs from nodes that no longer have edges...')
print(f'{len(ok_ids):,} IDs found in edges')
print(f"{len(set(edges['start_id']).union(set(edges['end_id']))):,} IDs found in nodes")

print('After cutting down to 6 Most Revelent Metanodes... ')
print(f'{num_metapaths(nodes, edges):,} Potential Metapaths')
print(f"Node Type                      Count:\n---------------------------------------\
\n{nodes['label'].value_counts()}")

print(f"Number of unique Edges Types{edges['type'].nunique()}")
print(f"Edge Type                 Count:\n-----------------------------------\
\n{edges['type'].value_counts()}")

print(f"Writing output file")
gt.add_colons(nodes).to_csv('../data/nodes_VER43_R_cons_6_metanode.csv', index=False)
gt.add_colons(edges).to_csv('../data/edges_VER43_R_cons_6_metanode.csv', index=False)

print(f'Complete processing 05_Keep_Six_relevant_metanodes.py')