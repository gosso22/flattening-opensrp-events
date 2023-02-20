#!/usr/bin/env python
# coding: utf-8

# In[2]:


import csv
import json
from collections import defaultdict
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column
from sqlalchemy.sql import select


# In[5]:


# Create a database engine and connect to the database
# engine = sqlalchemy.create_engine('postgresql://data_warehouse:strong_pass@localhost:5434/database_name')
engine = create_engine('postgresql://data_warehouse:strong_pass@localhost:5434/database_name')


# In[7]:


# Connect to the database
connection = engine.connect()


# In[9]:


raw_event_metadata = MetaData()
raw_event_table = Table('raw_events', raw_event_metadata, schema='opensrp')


# In[10]:


raw_event_select_stmnt = select('*').select_from(raw_event_table)


# In[11]:


raw_events_results = connection.execute(raw_event_select_stmnt).fetchall()


# In[12]:


list_of_raw_json = []
for raw_json in raw_events_results:
    list_of_raw_json.append(raw_json[1])


# In[2]:


# This is for running against raw csv file (You dont need to run this)
csv_file_path = '/home/gosso/Downloads/raw_events_202302082140.csv'

list_of_raw_json_from_csv = []
with open(csv_file_path, mode='r') as infile:
    csv_reader = csv.DictReader(infile)
    for line in csv_reader:
        second_field_name = csv_reader.fieldnames[1]
        parsed_json = json.loads(line[second_field_name])
        list_of_obs.append(parsed_json)


# In[ ]:


def get_child_unique_id(full_json):
    for obs in full_json['obs']:
        if obs['formSubmissionField'] == 'repeatvalueslist':
            child_repeat_dict = json.loads(obs['values'][0])
            child_ids = []
            for child_info in child_repeat_dict:
                child_ids.append(child_info['unique_id'])
            return child_ids
    
def get_human_readable_value(obs_dict):
    concat_value = ''
    for item_index in range(len(obs_dict)):
        try:
            if isinstance(obs_dict[item_index], list):
                try:
                    if item_index == 0:
                        concat_value += obs_dict[item_index][0]
                    else:
                    
                        concat_value += ', ' + obs_dict[item_index][0]
                except IndexError:
                    concat_value += ', ' + 'None'
            else:
                if item_index == 0:
                    concat_value += obs_dict[item_index]
                else:
                    concat_value += ', ' + obs_dict[item_index]
        except TypeError:
            concat_value = "None"
    return concat_value

new_list_of_jsons = []
location_id = ''
obs_list = {}
for i in range(len(list_of_raw_json)):
    only_one_json = list_of_raw_json[i]
    try:
        form_id = only_one_json.get('_id')
        base_entity_id = only_one_json.get('baseEntityId')
        event_type = only_one_json.get('eventType')
        provider_id = only_one_json.get('providerId')
        location_id = only_one_json.get('locationId')
        child_unique_id = []
        if event_type == 'Pregnancy Outcome':
            child_unique_id = get_child_unique_id(only_one_json)
        obs_list = {}
        for obs in only_one_json['obs']:
            field_name = ''
            data_point_value = ''
            if obs['formSubmissionField']:
                field_name = obs['formSubmissionField']
                if field_name == 'same_as_fam_name_35c1ce27955141528b56cd197d906a0f':
                    print(i)
                if child_unique_id is not None:
                    if len(child_unique_id) > 0:
                        for j in range(len(child_unique_id)):
                            if child_unique_id[j] in field_name:
                                field_name = field_name.replace('_' + child_unique_id[j], f'_child_{j+1}')
            if obs['humanReadableValues']:
                data_point_value = get_human_readable_value(obs['humanReadableValues'])
            else:
                if obs['values']:
                    data_point_value = get_human_readable_value(obs['values'])
            if len(obs_list) > 0:
                if any(field_name not in d for d in obs_list):
                    obs_list[field_name] = data_point_value
            else:
                obs_list[field_name] = data_point_value
        
    except KeyError as ke:
        print('Key not there: ', ke)
    the_temp_json = {'form_id': form_id, 
                     'location_id': location_id, 
                     'base_entity_id': base_entity_id, 
                     'event_type': event_type, 
                     'provider_id': provider_id, 
                     'obs': obs_list
                    }
    new_list_of_jsons.append(the_temp_json)

wh_df = pd.json_normalize(new_list_of_jsons)
#obs_df = pd.json_normalize(["id","danger_signs_present", "end", "save_n_refer"], wh_df['obs'])
#whole_thing_df = pd.concat([wh_df[["form_id", "base_entity_id"]].reset_index(), obs_df], axis=1)


# In[15]:


wh_df.to_excel('/home/gosso/Documents/Dashboard Data Work/flattened_raw_kk_test_data.xlsx', index=False)

