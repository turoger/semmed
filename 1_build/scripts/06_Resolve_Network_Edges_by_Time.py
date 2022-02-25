import os
import pickle
import pandas as pd
from tqdm import tqdm, tqdm_pandas

import warnings
warnings.filterwarnings("ignore")
from hetnet_ml import graph_tools as gt

print('')
print('')
print('Running 06_Resolve_Network_Edges_by_Time')

#### Build a final ID to year Map
print('Load PMID to year from NLM, PMC, EUR, and EBI')
nlm = pickle.load(open('../data/pmid_to_year_NLM.pkl', 'rb'))
pmc = pickle.load(open('../data/pmid_to_year_PMC.pkl', 'rb'))
eur = pickle.load(open('../data/pmid_to_year_Eur.pkl', 'rb'))
ebi = pickle.load(open('../data/pmid_to_year_EBI.pkl', 'rb'))

print('Reformatting all loaded PMID from str:str to str:ID')
# NLM
for k, v in tqdm(nlm.items()):
    nlm[k] = int(v)
#EUR
for k, v in tqdm(eur.items()):
    try:
        eur[k] = int(v)
    except:
        pass
#EBI
for k, v in tqdm(ebi.items()):
    ebi[k] = int(v.split('-')[0])
#PMC
# order of importance right to left. (pmc values will replace all others)
id_to_year = {**eur, **nlm, **ebi, **pmc}
id_to_year = {str(k): v for k, v in id_to_year.items()}

print(f"Getting nodes and edges file")
tqdm.pandas()
nodes = gt.remove_colons(pd.read_csv('../data/nodes_VER43_R_cons_6_metanode.csv'))
edges = gt.remove_colons(pd.read_csv('../data/edges_VER43_R_cons_6_metanode.csv'))

print(f'Remove edges with 0 pmid counts')
edges1 = edges.query('n_pmids == 1')
edges2 = edges.query('n_pmids > 1')
# process pmids for edges with only 1 reference
edges1['pmids'] = edges1['pmids'].apply(lambda x: [int(x[1:-1])])
edges1['pmids'] = edges1['pmids'].apply(lambda x: [str(i) for i in x])
# process pmids for edges with more than 1 reference
edges2['pmids'] = edges2['pmids'].apply(lambda x:x[1:-1].split(', '))


# concatenate the files
edges_no_zero = pd.concat([edges1,edges2])
print('Get publication dates for each edges')
# Ideally just want minimum year for a given edge, so supplying 9999 for not found pmids will
# allow min() to be used on the resulting list.
edges_no_zero['pub_years'] = edges_no_zero['pmids'].progress_apply(lambda pmids: [id_to_year.get(str(p), 9999) for p in pmids])
print(f'Get first date published for each indication')
edges_no_zero['first_pub'] = edges_no_zero['pub_years'].progress_apply(min)


print(f'Importing indication file to get dates')
indications = pd.read_csv('../data/indications_nodemerge.csv')

print(f'Number of Indications {len(indications):,}')
print(f'Dropping Indications withouht approval year')
indications.dropna(subset=['approval_year'], inplace=True)
indications['approval_year'] = indications['approval_year'].astype(int)
print(f'Number of remaining Indications {len(indications):,}')


print('Splitting nodes and edge files by year from 1950 - Current')
base_dir = '../data/time_networks-6_metanode'
def get_year_category(diff):
    if diff > 20:
        return '20+ After'
    elif diff >= 15 and diff < 20:
        return '15-20 After'
    elif diff >= 10 and diff < 15:
        return '10-15 After'
    elif diff >= 5 and diff < 10:
        return '5-10 After'
    elif diff >= 0 and diff < 5:
        return '0-5 After'
    elif diff >= -5 and diff < 0:
        return '0-5 Before'
    elif diff >= -10 and diff < -5:
        return '5-10 Before'
    elif diff >= -15 and diff < -10:
        return '10-15 Before'
    elif diff >= -20 and diff < -15:
        return '15-20 Before'
    elif diff < -20:
        return '20+ Before'
def plot_figure(indications, year, out_dir):
    
    order = ['20+ Before',
         '15-20 Before',
         '10-15 Before',
         '5-10 Before',
         '0-5 Before',
         '0-5 After',
         '5-10 After',
         '10-15 After',
         '15-20 After',
         '20+ After']
    
    
    plt.clf()
    plt.figure(figsize=(6, 4.5))

    f = sns.countplot(x='year_cat', data=indications, order=order)
    
    plt.ylabel('Count')
    plt.xlabel('Years from approval and {}'.format(year))
    plt.xticks(rotation=45)

    xlim = f.axes.get_xlim()
    x_mid = (xlim[1] + xlim[0]) / 2

    tick_diff = f.get_yticks()[1] - f.get_yticks()[0]
    y_height = f.get_yticks()[-2] - tick_diff/5 

    plt.text(3*x_mid/2, y_height, 'Total indications:\n          {}'.format(len(indications)))


    plt.title('Distribution of approval years for {}'.format(year))
    plt.tight_layout()
    plt.savefig(os.path.join(out_dir, 'ind-distribution.png'));

    
for year in tqdm(range(1950, 2022, 1)):

    # Define the save directory
    out_dir = os.path.join(base_dir, str(year))

    # Make sure the save directory exists, if not, make it
    try:
        os.stat(out_dir)
    except:
        os.makedirs(out_dir)       
    
    # Filter the edges by year
    e_filt = edges_no_zero.query('first_pub <= @year')

    # Keep only nodes that have edges joining them
    node_ids = set(e_filt['start_id']).union(set(e_filt['end_id']))
    n_filt = nodes.query('id in @node_ids')

    # Keep only indications that have both the compound and disease still existing in the network
    ind_filt = indications.query('compound_semmed_id in @node_ids and disease_semmed_id in @node_ids').reset_index(drop=True)

    # Determine the difference between the current year and approval
    ind_filt['year_diff'] = ind_filt['approval_year'] - year
    ind_filt['year_cat'] = ind_filt['year_diff'].apply(get_year_category)

    print("Year: {}\nIDs: {:,}\nNodes: {:,}\nEdges: {:,}\nIndications: {:,}\n\n".format(
        year, len(node_ids), len(n_filt), len(e_filt), len(ind_filt)))
    
    # Save the network, indications, and summary figure
    (gt.add_colons(n_filt, id_name='cui')
       .to_csv(os.path.join(out_dir, 'nodes.csv'), index=False))
    (gt.add_colons(e_filt, col_types={'n_pmids':'INT', 'first_pub':'INT'})
       .to_csv(os.path.join(out_dir, 'edges.csv'), index=False))
    ind_filt.to_csv(os.path.join(out_dir, 'indications.csv'), index=False)
    #plot_figure(ind_filt, year, out_dir);    

print('Done running 06_Resolve_Network_Edges_by_Time.py')