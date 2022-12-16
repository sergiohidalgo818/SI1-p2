
import ast
import os
import sys, traceback
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


if __name__ == "__main__":
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    mycol = database.getMongoCollection(client)

    

    database.mongoDBCloseConnect(client)


