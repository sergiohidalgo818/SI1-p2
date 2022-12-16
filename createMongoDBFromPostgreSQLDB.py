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
import math
import datetime
import database

# configurar el motor de sqlalchemy
db_engine = create_engine("postgresql://alumnodb:alumnodb@localhost/si1", echo=False)
db_meta = MetaData(bind=db_engine)
# cargar una tabla
db_table_movies = Table('imdb_movies', db_meta, autoload=True, autoload_with=db_engine)


def getImdb_UKrelatedmovies(title, genres): 
   
    try:
        db_conn = None
        db_conn = db_engine.connect()

        query = str("Select replace(substring(mo.movietitle,1, length(mo.movietitle) - 7), '(', '') as title, mo.year, mo.ratingcount, STRING_AGG(mg.genre, ', ') as genres from imdb_moviegenres mg join imdb_movies mo on mg.movieid = mo.movieid where replace(substring(mo.movietitle,1, length(mo.movietitle) - 7), '(', '') != %s  and mg.movieid in ( Select mo.movieid from imdb_movies mo join imdb_moviecountries im ON im.country='UK' and im.movieid = mo.movieid order by year desc limit 400) group by title, mo.year, mo.ratingcount order by mo.year desc;")
            
        db_result = db_conn.execute(query, title)


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
        db_result = db_conn.execute("Select replace(substring(mo.movietitle,1, length(mo.movietitle) - 7), '(', '') as title, z.genres, mo.year, mo.ratingcount, mo.ratingmean, x.directors, y.actors from imdb_movies mo join ( select movieid , STRING_AGG(concat('\"' , md.directorname , '\"'), ', ') as directors from imdb_directormovies dm join imdb_directors md on dm.directorid = md.directorid where movieid in ( Select mo.movieid from imdb_movies mo join imdb_moviecountries im ON im.country='UK' and im.movieid = mo.movieid order by year desc) group by dm.movieid) as x on mo.movieid = x.movieid join( select movieid, STRING_AGG(concat('\"', ma.actorname , '\"'), ', ') as actors from imdb_actormovies am join imdb_actors ma on am.actorid = ma.actorid where am.movieid in ( Select mo.movieid from imdb_movies mo join imdb_moviecountries im ON im.country='UK' and im.movieid = mo.movieid order by year desc ) group by am.movieid) as y on mo.movieid = y.movieid join ( select movieid, STRING_AGG(concat('\"',mg.genre, '\"'), ', ') as genres from imdb_moviegenres mg where mg.movieid in ( Select mo.movieid from imdb_movies mo join imdb_moviecountries im ON im.country='UK' and im.movieid = mo.movieid order by year desc) group by movieid) as z on mo.movieid = z.movieid where mo.movieid in ( Select mo.movieid from imdb_movies mo join imdb_moviecountries im ON im.country='UK' and im.movieid = mo.movieid order by year desc) group by title, mo.year , mo.movietitle, x.directors, mo.ratingcount, mo.ratingmean, y.actors, z.genres order by mo.year desc, title desc limit 400;")

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


    print('migrating.json')
    query = getImdb_UKtitle2actors()
    newUk = []
    for row in query:
        d = dict()
        d['title'] = str(row[0])

        d['genres'] = [str(row[1])]
        d['genres']= str(d['genres']).replace("'", "")
        d['genres']= str(d['genres']).replace("\\", "")
        d['genres'] = ast.literal_eval(d['genres'])
        
        d['year'] = int(row[2])
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


        query2 = getImdb_UKrelatedmovies(d['title'], d['genres'])

        
        related = list()
        i=0
        for row2 in query2:
            if(i==10):
                break
            
            
                
            j = len(d['genres'])
            points = 0
            while(j>0):
                j-=1
                if(str(d['genres'][-1]) in str(row2[3])):
                    points+=1


                if(points >= math.ceil(len(d['genres'])/2)):

                    x = dict()
                    x['title'] = row2[0]    
                    x['year'] = int(row2[1])
                    x['average_rating'] = row2[2]
                    related.append(x)
                    i+=1
                    break

        d['related_movies'] =[related]
        newUk.append(d)
    print('Generating title2actors.json')           
    with open('mongojs/title2actors.json', 'w') as f:
        f.write(json.dumps(newUk, indent = 5))
   
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")

    if 'si1' in myclient.list_database_names():
        myclient.drop_database('si1')

    mydb = myclient["si1"]
    
    mycol = mydb["topUK"]
    mycol.insert_many(newUk)

    database.mongoDBCloseConnect(myclient)


    