# -*- coding: utf-8 -*-

import os
import sys
import traceback
import time

from sqlalchemy import create_engine
from pymongo import MongoClient
import pymongo

# configurar el motor de sqlalchemy
db_engine = create_engine("postgresql://alumnodb:alumnodb@localhost/si1",
                          echo=False, execution_options={"autocommit": False})

# Crea la conexión con MongoDB
mongo_client = MongoClient()


def getMongoCollection(mongoDB_client:MongoClient):
    mongo_db = mongoDB_client.si1
    return mongo_db.topUK


def getMongoClient():
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    return myclient

def getComedymongo(mycol):
    return mycol.find({
        "$and": [{
            "year": {"$gte": 1990, "$lte": 1992}
        },
            {
            "genres": {"$regex": "Comedy"}
        }
        ]
    })

def getActionmongo(mycol):
    return mycol.find({
        "$and": [
        {
            "title": {"$regex": "The$"}
        }, 
        {
            "year": {"$in": [1995, 1997, 1998]}
        }
        ]
    })

def get2Actors(mycol):
    return mycol.find({
        "$and": [
        {
            "actors": {"$all": ["McAree, Darren", "Lockett, Katie"]} 
        }
        ]
    })



def mongoDBCloseConnect(mongoDB_client):
    mongoDB_client.close()


def dbConnect():
    return db_engine.connect()


def dbCloseConnect(db_conn):
    db_conn.close()


def delState(state, bFallo, bSQL, duerme, bCommit):

    # Array de trazas a mostrar en la página
    dbr = []

    # TODO: Ejecutar consultas de borrado
    # - ordenar consultas según se desee provocar un error (bFallo True) o no
    # - ejecutar commit intermedio si bCommit es True
    # - usar sentencias SQL ('BEGIN', 'COMMIT', ...) si bSQL es True
    # - suspender la ejecución 'duerme' segundos en el punto adecuado para forzar deadlock
    # - ir guardando trazas mediante dbr.append()

    try:
        # TODO: ejecutar consultas
        pass
    except Exception as e:
        # TODO: deshacer en caso de error
        pass
    else:
        # TODO: confirmar cambios si todo va bien
        pass

    return dbr
