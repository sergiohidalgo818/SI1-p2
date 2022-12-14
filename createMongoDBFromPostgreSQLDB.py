# -*- coding: utf-8 -*-
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

import datetime

# configurar el motor de sqlalchemy
db_engine = create_engine("postgresql://alumnodb:alumnodb@localhost/si1", echo=False)
db_meta = MetaData(bind=db_engine)
# cargar una tabla
db_table_movies = Table('imdb_movies', db_meta, autoload=True, autoload_with=db_engine)


def getImdb_UKrelatedmovies(genre): 
   
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


def getImdb_UKtitle2actors(): 
   
    try:
        db_conn = None
        db_conn = db_engine.connect()
        db_result = db_conn.execute("Select replace(substring(mo.movietitle,1, length(mo.movietitle) - 7), '(', '') as title, STRING_AGG(concat('\"',mg.genre, '\"'), ', ') as genres, mo.year, mo.ratingcount, mo.ratingmean, x.directors, y.actors from imdb_moviegenres mg join imdb_movies mo on mg.movieid = mo.movieid join ( select replace(substring(mo.movietitle,1, length(mo.movietitle) - 7), '(', '') as title2, STRING_AGG(concat('\"' , md.directorname , '\"'), ', ') as directors from imdb_directormovies dm join imdb_movies mo on mo.movieid = dm.movieid join imdb_directors md on dm.directorid = md.directorid where mo.movieid in ( Select mo.movieid from imdb_movies mo join imdb_moviecountries im ON im.country='UK' and im.movieid = mo.movieid order by year desc limit 400) group by mo.year, mo.movieid order by mo.year desc) as x on replace(substring(mo.movietitle,1, length(mo.movietitle) - 7), '(', '') = x.title2 join( select replace(substring(mo.movietitle,1, length(mo.movietitle) - 7), '(', '') as title2, STRING_AGG(concat('\"', ma.actorname , '\"'), ', ') as actors from imdb_actormovies am join imdb_movies mo on mo.movieid = am.movieid join imdb_actors ma on am.actorid = ma.actorid where mo.movieid in ( Select mo.movieid from imdb_movies mo join imdb_moviecountries im ON im.country='UK' and im.movieid = mo.movieid order by year desc limit 400) group by mo.year, mo.movieid order by mo.year desc) as y on replace(substring(mo.movietitle,1, length(mo.movietitle) - 7), '(', '') = y.title2 where mg.movieid  in ( Select mo.movieid from imdb_movies mo join imdb_moviecountries im ON im.country='UK' and im.movieid = mo.movieid order by year desc limit 400) group by title, mo.year , mo.movietitle, x.directors, mo.ratingcount, mo.ratingmean, y.actors order by mo.year desc, title desc;")

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


    print('Generating title2actors.json')
    query = getImdb_UKtitle2actors()
    newUk = []
    for row in query:
        d = dict()
        d['title'] = row[0]

        d['genres'] = [str(row[1])]
        d['genres']= str(d['genres']).replace("'", "")
        d['genres']= str(d['genres']).replace("\\", "")
        d['genres'] = ast.literal_eval(d['genres'])
        
        d['year'] = row[2]
        d['number_of_votes'] = row[3]
        d['average_rating'] = row[4]
        d['directors'] = [str(row[5])]
        d['directors']= str(d['directors']).replace("'", "")
        d['directors']= str(d['directors']).replace("\\", "")
        d['directors'] = ast.literal_eval(d['directors'])
        d['actors'] = [str(row[6])]
        d['actors']= str(d['actors']).replace("'", "")
        d['actors']= str(d['actors']).replace("\\", "")
        d['actors'] = ast.literal_eval(d['actors'])

        for genre in d['genres']:
            print(genre)
        
        newUk.append(d)

        
    with open('mongojs/title2actors.json', 'w') as f:
        f.write(json.dumps(newUk, indent = 5))
   