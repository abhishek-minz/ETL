#ETL with python
#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import pprint
import pymongo

from bson.json_util import dumps


#### Pulling files from MongoDB
from pymongo import MongoClient
client = MongoClient('mongodb://localhost:27017/')

#Connecting to the water_contamination cluster
db=client.water_contamination


#Connecting to collection violations
violations=list(db.violations.find())
violations=pd.DataFrame(violations)
del violations['_id']


#Retriving the dataframe as a csv file
violations = violations.to_csv('violation_summaries.csv', sep = ',',index=0,header=True)

#Connecting to collection suppliers
suppliers=list(db.suppliers.find())
suppliers=pd.DataFrame(suppliers)
del suppliers['_id']

#Retriving the dataframe as a csv file
suppliers = suppliers.to_csv('suppliers.csv', sep = ',',index=0,header=True)

#Connecting to collection locations
locations=list(db.locations.find())
locations=pd.DataFrame(locations)
del locations['_id']

#Retriving the dataframe as a csv file
locations = locations.to_csv('zip_codes.csv', sep = ',',index=0,header=True)

#Connecting to collection contaminants
contaminants=list(db.contaminants.find())
contaminants=pd.DataFrame(contaminants)
del contaminants['_id']

#Retriving the dataframe as a csv file
contaminants = contaminants.to_csv('contaminants.csv', sep = ',',index=0,header=True)

