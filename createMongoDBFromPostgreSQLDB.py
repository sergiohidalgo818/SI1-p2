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


def getImdb_UKmovietitle(): 
   
    try:
        db_conn = None
        db_conn = db_engine.connect()
        db_result = db_conn.execute("Select replace(substring(movietitle,1, length(movietitle) - 7), '(', '') from imdb_movies mo join imdb_moviecountries im ON im.country='UK' and im.movieid = mo.movieid order by year desc")

        return db_result.fetchall()
    except:
        if db_conn is not None:
            db_conn.close()
        print("Exception in DB access:")
        print("-"*60)
        traceback.print_exc(file=sys.stderr)
        print("-"*60)

        return 'Something is broken'


def getImdb_UKmoviegenres(): 
   
    try:
        db_conn = None
        db_conn = db_engine.connect()
        db_result = db_conn.execute("Select replace(substring(mo.movietitle,1, length(mo.movietitle) - 7), '(', '') as title,  mg.genre from imdb_moviegenres mg join imdb_movies mo on mg.movieid = mo.movieid where mg.movieid  in (Select mo.movieid from imdb_movies mo join imdb_moviecountries im ON im.country='UK' and im.movieid = mo.movieid order by year desc limit 400) order by mo.year desc, title desc ;")

        return db_result.fetchall()
    except:
        if db_conn is not None:
            db_conn.close()
        print("Exception in DB access:")
        print("-"*60)
        traceback.print_exc(file=sys.stderr)
        print("-"*60)

        return 'Something is broken'


def getImdb_UKmovietitlegenre(): 
   
    try:
        db_conn = None
        db_conn = db_engine.connect()
        db_result = db_conn.execute("Select mg.genre, mg.movieid from imdb_moviegenres mg where mg.movieid  in (Select mo.movieid from imdb_movies mo join imdb_moviecountries im ON im.country='UK' and im.movieid = mo.movieid order by year desc)")

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


    print('Generating imdb_moviesname.json')
    query = getImdb_UKmovietitle()
    newUk = []
    for row in query:
        d = dict()
        d['movietitle'] = row[0]
        newUk.append(d)
    with open('mongojs/imdb_moviesname.json', 'w') as f:
        f.write(json.dumps(newUk, indent = 4))

        
    print('Generating imdb_genres.json')
    query = getImdb_UKmoviegenres()
    daux=[]
    aux=query[0][1]
    for row in query:
        d = dict()
        if(row[1] == aux):
            daux.append(row[0])
        else:
            d['genre'] = daux
            newUk.append(d)
        aux=row[1]
    with open('mongojs/imdb_genres.json', 'w') as f:
        f.write(json.dumps(newUk, indent = 4))
    print(newUk)

