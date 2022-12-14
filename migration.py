# -*- coding: utf-8 -*-

import os
import sys, traceback
from sqlalchemy import and_, create_engine, update
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, text
from sqlalchemy.sql import select
import json
import collections
import pymongo
from pymongo import MongoClient

import datetime

# configurar el motor de sqlalchemy
db_engine = create_engine("postgresql://alumnodb:alumnodb@localhost/si1", echo=False)
db_meta = MetaData(bind=db_engine)
# cargar una tabla
db_table_movies = Table('imdb_movies', db_meta, autoload=True, autoload_with=db_engine)


def getCustomers(): 
   
    try:
        db_conn = None
        db_conn = db_engine.connect()
        db_result = db_conn.execute("Select * from customers")

        return db_result.fetchall()
    except:
        if db_conn is not None:
            db_conn.close()
        print("Exception in DB access:")
        print("-"*60)
        traceback.print_exc(file=sys.stderr)
        print("-"*60)

        return 'Something is broken'




def getImdb_actormovies(): 
   
    try:
        db_conn = None
        db_conn = db_engine.connect()
        db_result = db_conn.execute("Select * from imdb_actormovies")

        return db_result.fetchall()
    except:
        if db_conn is not None:
            db_conn.close()
        print("Exception in DB access:")
        print("-"*60)
        traceback.print_exc(file=sys.stderr)
        print("-"*60)

        return 'Something is broken'




def getImdb_actors(): 
   
    try:
        db_conn = None
        db_conn = db_engine.connect()
        db_result = db_conn.execute("Select * from imdb_actors")

        return db_result.fetchall()
    except:
        if db_conn is not None:
            db_conn.close()
        print("Exception in DB access:")
        print("-"*60)
        traceback.print_exc(file=sys.stderr)
        print("-"*60)

        return 'Something is broken'




def getImdb_directormovies(): 
   
    try:
        db_conn = None
        db_conn = db_engine.connect()
        db_result = db_conn.execute("Select * from imdb_directormovies")

        return db_result.fetchall()
    except:
        if db_conn is not None:
            db_conn.close()
        print("Exception in DB access:")
        print("-"*60)
        traceback.print_exc(file=sys.stderr)
        print("-"*60)

        return 'Something is broken'




def getImdb_directors(): 
   
    try:
        db_conn = None
        db_conn = db_engine.connect()
        db_result = db_conn.execute("Select * from imdb_directors")

        return db_result.fetchall()
    except:
        if db_conn is not None:
            db_conn.close()
        print("Exception in DB access:")
        print("-"*60)
        traceback.print_exc(file=sys.stderr)
        print("-"*60)

        return 'Something is broken'




def getImdb_moviecountries(): 
   
    try:
        db_conn = None
        db_conn = db_engine.connect()
        db_result = db_conn.execute("Select * from imdb_moviecountries")

        return db_result.fetchall()
    except:
        if db_conn is not None:
            db_conn.close()
        print("Exception in DB access:")
        print("-"*60)
        traceback.print_exc(file=sys.stderr)
        print("-"*60)

        return 'Something is broken'




def getImdb_moviegenres(): 
   
    try:
        db_conn = None
        db_conn = db_engine.connect()
        db_result = db_conn.execute("Select * from imdb_moviegenres")

        return db_result.fetchall()
    except:
        if db_conn is not None:
            db_conn.close()
        print("Exception in DB access:")
        print("-"*60)
        traceback.print_exc(file=sys.stderr)
        print("-"*60)

        return 'Something is broken'

def getImdb_movielanguages(): 
   
    try:
        db_conn = None
        db_conn = db_engine.connect()
        db_result = db_conn.execute("Select * from imdb_movielanguages")

        return db_result.fetchall()
    except:
        if db_conn is not None:
            db_conn.close()
        print("Exception in DB access:")
        print("-"*60)
        traceback.print_exc(file=sys.stderr)
        print("-"*60)

        return 'Something is broken'




def getImdb_movies(): 
   
    try:
        db_conn = None
        db_conn = db_engine.connect()
        db_result = db_conn.execute("Select * from imdb_movies")

        return db_result.fetchall()
    except:
        if db_conn is not None:
            db_conn.close()
        print("Exception in DB access:")
        print("-"*60)
        traceback.print_exc(file=sys.stderr)
        print("-"*60)

        return 'Something is broken'




def getInventory(): 
   
    try:
        db_conn = None
        db_conn = db_engine.connect()
        db_result = db_conn.execute("Select * from inventory")

        return db_result.fetchall()
    except:
        if db_conn is not None:
            db_conn.close()
        print("Exception in DB access:")
        print("-"*60)
        traceback.print_exc(file=sys.stderr)
        print("-"*60)

        return 'Something is broken'




def getOrderdetail(): 
   
    try:
        db_conn = None
        db_conn = db_engine.connect()
        db_result = db_conn.execute("Select * from orderdetail")

        return db_result.fetchall()
    except:
        if db_conn is not None:
            db_conn.close()
        print("Exception in DB access:")
        print("-"*60)
        traceback.print_exc(file=sys.stderr)
        print("-"*60)

        return 'Something is broken'




def getOrders(): 
   
    try:
        db_conn = None
        db_conn = db_engine.connect()
        db_result = db_conn.execute("Select * from orders")

        return db_result.fetchall()
    except:
        if db_conn is not None:
            db_conn.close()
        print("Exception in DB access:")
        print("-"*60)
        traceback.print_exc(file=sys.stderr)
        print("-"*60)

        return 'Something is broken'



def getProducts(): 
   
    try:
        db_conn = None
        db_conn = db_engine.connect()
        db_result = db_conn.execute("Select * from products")

        return db_result.fetchall()
    except:
        if db_conn is not None:
            db_conn.close()
        print("Exception in DB access:")
        print("-"*60)
        traceback.print_exc(file=sys.stderr)
        print("-"*60)

        return 'Something is broken'




if __name__ == "__main__":

    print('Generating customers.json')
    query = getCustomers()
    customers = []
    for row in query:
        d = dict()
        d['customerid'] = row[0]
        d['address'] = row[1]
        d['email'] = row[2]
        d['creditcard'] = row[3]
        d['username'] = row[4]
        d['password'] = row[5]
        customers.append(d)
    with open('mongojs/customers.json', 'w') as f:
        f.write(json.dumps(customers, indent = 4))


    print('Generating imdb_actormovies.json')
    query = getImdb_actormovies()
    imdb_actormovies = []
    for row in query:
        d = dict()
        d['actorid'] = row[0]
        d['movieid'] = row[1]
        d['character'] = row[2]
        d['ascharacter'] = row[3]
        d['isvoice'] = row[4]
        d['isarchivefootage'] = row[5]
        d['isuncredited'] = row[6]
        d['creditsposition'] = row[7]
        imdb_actormovies.append(d)
    with open('mongojs/imdb_actormovies.json', 'w') as f:
        f.write(json.dumps(imdb_actormovies, indent = 4))


    print('Generating imdb_actors.json')
    query = getImdb_actors()
    imdb_actors = []
    for row in query:
        d = dict()
        d['actorid'] = row[0]
        d['actorname'] = row[1]
        d['gender'] = row[2]
        imdb_actors.append(d)
    with open('mongojs/imdb_actors.json', 'w') as f:
        f.write(json.dumps(imdb_actors, indent = 4))



    print('Generating imdb_directormovies.json')
    query = getImdb_directormovies()
    imdb_directormovies = []
    for row in query:
        d = dict()
        d['directorid'] = row[0]
        d['movieid'] = row[1]
        d['numpartitipation'] = row[2]
        d['ascharacter'] = row[3]
        d['participation'] = row[4]
        d['isarchivefootage'] = row[5]
        d['isuncredited'] = row[6]
        d['iscodirector'] = row[7]
        d['ispilot'] = row[8]
        d['ischief'] = row[9]
        d['ishead'] = row[10]
        imdb_directormovies.append(d)
    with open('mongojs/imdb_directormovies.json', 'w') as f:
        f.write(json.dumps(imdb_directormovies, indent = 4))


    print('Generating imdb_directors.json')
    query = getImdb_directors()
    imdb_directors = []
    for row in query:
        d = dict()
        d['directorid'] = row[0]
        d['directorname'] = row[1]
        imdb_directors.append(d)
    with open('mongojs/imdb_directors.json', 'w') as f:
        f.write(json.dumps(imdb_directors, indent = 4))


    print('Generating imdb_moviecountries.json')
    query = getImdb_moviecountries()
    imdb_moviecountries = []
    for row in query:
        d = dict()
        d['movieid'] = row[0]
        d['country'] = row[1]
        imdb_moviecountries.append(d)
    with open('mongojs/imdb_moviecountries.json', 'w') as f:
        f.write(json.dumps(imdb_moviecountries, indent = 4))



    print('Generating imdb_moviegenres.json')
    query = getImdb_moviegenres()
    imdb_moviegenres = []
    for row in query:
        d = dict()
        d['movieid'] = row[0]
        d['genre'] = row[1]
        imdb_moviegenres.append(d)
    with open('mongojs/imdb_moviegenres.json', 'w') as f:
        f.write(json.dumps(imdb_moviegenres, indent = 4))


    print('Generating imdb_movielanguages.json')
    query = getImdb_movielanguages()
    imdb_moviegenres = []
    for row in query:
        d = dict()
        d['movieid'] = row[0]
        d['language'] = row[1]
        d['extrainformation'] = row[2]
        imdb_moviegenres.append(d)
    with open('mongojs/imdb_movielanguages.json', 'w') as f:
        f.write(json.dumps(imdb_moviegenres, indent = 4))


    print('Generating imdb_movies.json')
    query = getImdb_movies()
    imdb_movies = []
    for row in query:
        d = dict()
        d['movieid'] = row[0]
        d['movietitle'] = row[1]
        d['movierelease'] = row[2]
        d['movietype'] = row[3]
        d['year'] = row[4]
        d['issuspended'] = row[5]
        imdb_movies.append(d)
    with open('mongojs/imdb_movies.json', 'w') as f:
        f.write(json.dumps(imdb_movies, indent = 4))


    print('Generating inventory.json')
    query = getInventory()
    inventory = []
    for row in query:
        d = dict()
        d['prod_id'] = row[0]
        d['stock'] = row[1]
        d['sales'] = row[2]
        inventory.append(d)
    with open('mongojs/inventory.json', 'w') as f:
        f.write(json.dumps(inventory, indent = 4))


    print('Generating orderdetail.json')
    query = getOrderdetail()
    orderdetail = []
    for row in query:
        d = dict()
        d['orderid'] = row[0]
        d['prod_id'] = row[1]
        d['price'] = row[2]
        d['quantity'] = row[3]
        orderdetail.append(d)
    with open('mongojs/orderdetail.json', 'w') as f:
        f.write(json.dumps(orderdetail, indent = 4))

    print('Generating orders.json')
    query = getOrders()
    orders = []
    for row in query:
        d = dict()
        d['orderid'] = row[0]
        d['orderdate'] = str(row[1])
        d['customerid'] = row[2]
        d['netamount'] = str(row[3])
        d['tax'] = str(row[4])
        d['totalamount'] = row[5]
        d['status'] = row[6]

        orders.append(d)

    
    with open('mongojs/orders.json', 'w') as f:
        f.write(json.dumps(orders, indent = 4))



    print('Generating products.json')
    query = getProducts()
    products = []
    for row in query:
        d = dict()
        d['prod_id'] = row[0]
        d['movieid'] = row[1]
        d['price'] = str(row[2])
        d['description'] = row[3]
        products.append(d)
    with open('mongojs/products.json', 'w') as f:
        f.write(json.dumps(products, indent = 4))



    myclient = pymongo.MongoClient("mongodb://localhost:27017/")

    mydb = myclient["si1"]

    mycol = mydb["customers"]
    mycol.insert_many(customers)

    mycol = mydb["imdb_actormovies"]
    mycol.insert_many(customers)

    mycol = mydb["imdb_actors"]
    mycol.insert_many(customers)

    mycol = mydb["imdb_directormovies"]
    mycol.insert_many(customers)

    mycol = mydb["imdb_directors"]
    mycol.insert_many(customers)

    mycol = mydb["imdb_moviecountries"]
    mycol.insert_many(customers)

    mycol = mydb["imdb_moviegenres"]
    mycol.insert_many(customers)

    mycol = mydb["imdb_movielanguages"]
    mycol.insert_many(customers)

    mycol = mydb["imdb_movies"]
    mycol.insert_many(customers)

    mycol = mydb["inventory"]
    mycol.insert_many(customers)

    mycol = mydb["orderdetail"]
    mycol.insert_many(customers)

    mycol = mydb["orders"]
    mycol.insert_many(customers)

    mycol = mydb["products"]
    mycol.insert_many(customers)





