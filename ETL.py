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


import pandas as pd  
import json
import csv
import pprint
import pymongo

from bson.json_util import dumps


# ### Loading the pre-processed files

violations = pd.read_csv (r'violations_cleaned.csv')

suppliers = pd.read_csv (r'suppliers_cleaned.csv')

locations = pd.read_csv (r'locations_cleaned.csv')
locations=locations.drop(['lat', 'long'], axis=1)

contaminants = pd.read_csv (r'contaminants_cleaned.csv')

# ### Loading data to mysql
#Creating a databse water_contaminantion in MYSQL database
import mysql.connector as msql
from mysql.connector import Error
try:
    conn = msql.connect(host='localhost', user='root',  
                        password='password')
    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute("CREATE DATABASE water_contamination")
        print("Database is created")
except Error as e:
    print("Error while connecting to MySQL", e)

#Creating a table locations in the database water_contamination
try:
    conn = msql.connect(host='localhost', database='water_contamination', user='root', password='password')
    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("You're connected to database: ", record)
        cursor.execute('DROP TABLE IF EXISTS locations;')
        print('Creating table....')
        cursor.execute("CREATE TABLE locations(location varchar(300), location_zipcode int, estimatedpopulation int, totalwages int, taxreturnsfiled int)")
        print("Table is created....")
        #looping through the data frame locations
        for i,row in locations.iterrows(): 
            sql = "INSERT INTO water_contamination.locations VALUES (%s,%s,%s,%s,%s)"
            cursor.execute(sql, tuple(row))
            print("Record inserted")
            conn.commit()
except Error as e:
            print("Error while connecting to MySQL", e)

#Creating a table suppliers in the database water_contamination
try:
    conn = msql.connect(host='localhost', database='water_contamination', user='root', password='password')
    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("You're connected to database: ", record)
        cursor.execute('DROP TABLE IF EXISTS suppliers;')
        print('Creating table....')
        cursor.execute("CREATE TABLE suppliers(supplier_id int, supplier_zipcode int, supplier_name varchar(300), location varchar(1000), number_of_people_served int)")
        print("Table is created....")
        #looping through the data frame suppliers
        for i,row in suppliers.iterrows(): 
            sql = "INSERT INTO water_contamination.suppliers VALUES (%s,%s,%s,%s,%s)"
            cursor.execute(sql, tuple(row))
            print("Record inserted")
            conn.commit()
except Error as e:
            print("Error while connecting to MySQL", e)

#Creating a table violations in the database water
try:
    conn = msql.connect(host='localhost', database='water_contamination', user='root', password='password')
    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("You're connected to database: ", record)
        cursor.execute('DROP TABLE IF EXISTS violations;')
        print('Creating table....')
        cursor.execute("CREATE TABLE violations(id int, violation varchar(300), date_of_violation date, location varchar(1000), supplier_name varchar(300), number_of_people_served varchar(300))")
        print("Table is created....")
        #looping through the data frame
        for i,row in violations.iterrows(): 
            sql = "INSERT INTO water_contamination.violations VALUES (%s,%s,%s,%s,%s,%s)"
            cursor.execute(sql, tuple(row))
            print("Record inserted")
            conn.commit()
except Error as e:
            print("Error while connecting to MySQL", e)


#Creating a table contaminants in the database water_contamination
try:
    conn = msql.connect(host='localhost', database='water_contamination', user='root', password='password')
    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("You're connected to database: ", record)
        cursor.execute('DROP TABLE IF EXISTS contaminants;')
        print('Creating table....')
        cursor.execute("CREATE TABLE contaminants(id int, contaminant varchar(300), health_limit_exceeded varchar(150), legal_limit_exceeded varchar(150), location varchar(1000), supplier_name varchar(300), number_of_people_served int)")
        print("Table is created....")
        #looping through the data frame contaminants
        for i,row in contaminants.iterrows(): 
            sql = "INSERT INTO water_contamination.contaminants VALUES (%s,%s,%s,%s,%s,%s,%s)"
            cursor.execute(sql, tuple(row))
            print("Record inserted")
            conn.commit()
except Error as e:
            print("Error while connecting to MySQL", e)
