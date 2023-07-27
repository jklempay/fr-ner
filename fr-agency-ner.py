#!/usr/bin/env python
import spacy
import os
import re
import pandas as pd
from collections import Counter

data_dir = '../../language_of_bureaucracy/data/FR_text/'

nlp = spacy.blank('en')
ruler = nlp.add_pipe("entity_ruler")

# Parse wikidata to create patterns for the entity ruler
df = pd.read_csv('data/wikidata_agencies.csv')
df = pd.melt(df, id_vars=['item'], value_vars=['itemLabel', 'langAlias'], value_name = 'pattern')
df = df.drop_duplicates()
df = df.dropna()
df = df.drop(columns=['variable'])
df = df.assign(label='AGENCY')
df = df.rename(columns={'item': 'id'})
df['id'] = df['id'].apply(lambda x: x.split('/')[-1])

patterns = df.to_dict(orient='records')
ruler.add_patterns(patterns)

# Parse wikidata to get keys
df = pd.read_csv('data/wikidata_keys.csv')
df['item'] = df['item'].apply(lambda x: x.split('/')[-1])
df.set_index('item', inplace=True)
keys = df.index.values.tolist()

ent_freqs = pd.DataFrame(index=keys)
ent_freqs.fillna(0, inplace=True)

# Group the data files by year to facilitate processing
dir_list = os.listdir(data_dir)
file_groups = {}
for file in dir_list:
    year = file[:4]
    if year not in file_groups:
        file_groups[year] = [file]
    else:
        file_groups[year].append(file)

for year, files in file_groups.items():

    print(f"Working on {year}...", end='\r')

    if year not in ent_freqs.columns:
        ent_freqs[year] = 0
    
    for file in files:
        file_path = data_dir + file
        with open(file_path, 'r', encoding='utf-8') as f:
            text = f.read()
        text = re.sub(r'\xad\n', '', text)
        text = ' '.join(text.split())
        data = [text[i:i+10000] for i in range(0, len(text), 10000)]
    
        docs = list(nlp.pipe(data))

        orglist = []
        for doc in docs:
            for ent in doc.ents:
                orglist.append(ent.ent_id_)
                
        c = Counter(orglist)
        for id, freq in c.most_common():
            ent_freqs.at[id, year] += freq
        
ent_freqs = df.join(ent_freqs)
        
ent_freqs.to_csv('data/ent_freqs.csv')
print('\nFinished!')
