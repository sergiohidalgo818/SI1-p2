
import ast
import os
import sys
import traceback
from sqlalchemy import and_, create_engine, update
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, text
from sqlalchemy.sql import select
import json
import collections
import pymongo
from pymongo import MongoClient
import math
import datetime
import pprint
import database
import pprint


client = database.mongo_client
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
 

db = myclient['si1'] 
collection = db['topUK'] 

query = database.getTest(collection)
print(query)
for i in query:
    print(i['title'])

database.mongoDBCloseConnect(myclient)
    