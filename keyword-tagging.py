from IPython.core.interactiveshell import InteractiveShell
InteractiveShell.ast_node_interactivity = "all"

import pandas as pd
import numpy as np
import re

df = pd.read_csv("data_filtered.csv", encoding = "ISO-8859-1") 
df = df.astype(str)
df["event_summary"] = df["event_summary"].str.lower()

#####dictionary
virtus_dict = pd.read_excel("Dictionary.xlsx")
virtus_dict = virtus_dict.fillna('').astype(str).apply(lambda x: x.str.lower())
virtus_dict['Tag'] = virtus_dict.Tag.str.replace(' ','_')

type(zip(virtus_dict["Keyword"], virtus_dict["Tag"]))
dict_list = dict(zip(virtus_dict["Keyword"], virtus_dict["Tag"]))

######

res = {}
for i, v in dict_list.items():
    res[v] = [i] if v not in res.keys() else res[v] + [i]
# print(res)
# res.keys()
d_lower = {key.lower():value.lower() for key, value in dict_list.items()}


######adding tags in new column separated by comma
df['matchlabels'] = df['event_summary'].apply(lambda x: ', '.join(
      set([d_lower[y] for y in d_lower.keys() if y in x])
    ))


######addings 1s and 0s in new columns
pattern = '|'.join(dict_list.keys())
matches = df['event_summary'].str.extractall(f"({pattern})", flags=re.IGNORECASE)[0]
tags = matches.str.lower().map({kw.lower():tag for kw, tag in dict_list.items()})
tags = tags.rename_axis(['line', 'match']).reset_index(name='tag').assign(value=1)
df = tags.pivot_table(index='line', columns='tag', values='value', fill_value=0).join(df[['id','matchlabels','event_summary']])

#####reordering columns
cols = df.columns.tolist()
cols = cols[-1:] + cols[:-1]
cols = cols[-1:] + cols[:-1]
cols = cols[-1:] + cols[:-1]
df = df[cols]
df




#function for same code above

data_filtered = "data_set.csv"
dictionary = "Dictionary.xlsx"
column_name = "event_summary"

def tagging_function(df1, df2, columnName):
    df = pd.read_csv(df1, encoding = "ISO-8859-1") 
    df = df.astype(str)
    df[columnName] = df[columnName].str.lower()
    id_col = [col for col in df.columns if 'id' in col]
    id_col = ''.join(map(str, id_col))
    virtus_dict = pd.read_excel(df2)
    virtus_dict = virtus_dict.fillna('').astype(str).apply(lambda x: x.str.lower())
    virtus_dict['Tag'] = virtus_dict.Tag.str.replace(' ','_')
    type(zip(virtus_dict["Keyword"], virtus_dict["Tag"]))
    dict_list = dict(zip(virtus_dict["Keyword"], virtus_dict["Tag"]))
    res = {}
    for i, v in dict_list.items():
        res[v] = [i] if v not in res.keys() else res[v] + [i]
    d_lower = {key.lower():value.lower() for key, value in dict_list.items()}

    df['matchlabels'] = df[columnName].apply(lambda x: ', '.join(
      set([d_lower[y] for y in d_lower.keys() if y in x])
    ))
    
    pattern = '|'.join(dict_list.keys())
    matches = df[columnName].str.extractall(f"({pattern})", flags=re.IGNORECASE)[0]
    tags = matches.str.lower().map({kw.lower():tag for kw, tag in dict_list.items()})
    tags = tags.rename_axis(['line', 'match']).reset_index(name='tag').assign(value=1)
    df = tags.pivot_table(index='line', columns='tag', values='value', fill_value=0).join(df[[id_col, 'matchlabels', columnName]])
    cols = df.columns.tolist()
    cols = cols[-1:] + cols[:-1]
    cols = cols[-1:] + cols[:-1]
    cols = cols[-1:] + cols[:-1]
    df = df[cols]
    return df.to_csv('functionCSV.csv')

tagging_function(data_filtered, dictionary, column_name)
