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


def getMongoCollection(mongoDB_client):
    mongo_db = mongoDB_client.si1
    return mongo_db.topUK

def getTest(mycol):
    return (mycol.find({
        "$and": [{
            "title": "Autumn Heart"
        }
        ]
    }))


def getComedymongo(mycol):
    return mycol.find({
        "$and": [{
            "year": {"$gte": 1990, "$lte": 1992}
        },
        {
            "genres": "Comedy"
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
            "genres": "Action"
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


def delOrderDetail(db_conn, state):
        
    try:
            
        query = str("delete from orderdetail odt using orders o, customers c where odt.orderid = o.orderid and o.customerid = c.customerid and c.state = %s;")
        db_conn.execute(query, state)


        return 'good'
    except:
        print("Exception in DB access:")
        print("-"*60)
        traceback.print_exc(file=sys.stderr)
        print("-"*60)

        return 'Something is broken'


def delOrders(db_conn, state):
        
    try:
        query = str("delete from orders o using customers c where o.customerid = c.customerid and c.state = %s;")
        db_conn.execute(query, state)


        return 'good'
    except:
        print("Exception in DB access:")
        print("-"*60)
        traceback.print_exc(file=sys.stderr)
        print("-"*60)

        return 'Something is broken'



def delCustomers(db_conn, state):
        

    try:
        query = str("delete from customers where state = %s;")
        db_conn.execute(query, state)


        return 'good'
    except:
        print("Exception in DB access:")
        print("-"*60)
        traceback.print_exc(file=sys.stderr)
        print("-"*60)

        return 'Something is broken'



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
        db_conn = dbConnect()
        dbr.append("Se está iniciando la transacción")
        if bSQL:
            dbr.append("Se ha elegido usar sentencias de SQL")
            db_conn.execute("begin;")
        else:
            dbr.append("Se ha elegido no usar sentencias de SQL")
            transaction = db_conn.begin()

        if bFallo:
            dbr.append("Se va a provocar un fallo")

            if str(delOrderDetail(db_conn, str(state))) == str('Something is broken'):
                dbr.append("Se ha borrado la tabla orderdetail incorrectamente")
                raise Exception
            else:
                dbr.append("Se ha borrado la tabla orderdetail correctamente")

            if bCommit == True:
                if bSQL:
                    db_conn.execute("commit;")
                    db_conn.execute("begin;")

                else:
                    transaction.commit()
                    transaction = db_conn.begin()

                dbr.append("Commit intermedio")

            
            if str(delCustomers(db_conn, str(state))) == str('Something is broken'):
                dbr.append("Se ha borrado la tabla customers incorrectamente")
                raise Exception
            else:
                dbr.append("Se ha borrado la tabla customers correctamente")
            
            if str(delOrders(db_conn, str(state))) == str('Something is broken'):
                dbr.append("Se ha borrado la tabla orders incorrectamente")
                raise Exception
            else:
                dbr.append("Se ha borrado la tabla orders correctamente")

        else:
           
            if str(delOrderDetail(db_conn, str(state))) == str('Something is broken'):
                dbr.append("Se ha borrado la tabla orderdetail incorrectamente")
                raise Exception
            else:
                dbr.append("Se ha borrado la tabla orderdetail correctamente")

            if bCommit == True:
                if bSQL:
                    db_conn.execute("commit;")
                    db_conn.execute("begin;")

                else:
                    transaction.commit()
                    transaction = db_conn.begin()

                dbr.append("Commit intermedio")
                
            if str(delOrders(db_conn, str(state))) == str('Something is broken'):
                dbr.append("Se ha borrado la tabla orders incorrectamente")
                raise Exception
            else:
                dbr.append("Se ha borrado la tabla orders correctamente")

            if str(delCustomers(db_conn, str(state))) == str('Something is broken'):
                dbr.append("Se ha borrado la tabla customers incorrectamente")
                raise Exception
            else:
                dbr.append("Se ha borrado la tabla customers correctamente")

    except Exception as e:
        dbr.append("La transacción ha fallado, se hará rollback") 
        if bSQL:
            db_conn.execute("rollback;")
        else:
            transaction.rollback()


        pass
    else:
        if bSQL:
            db_conn.execute("commit;")
        else:
            transaction.commit()
        dbr.append("La transacción ha ido correctamente")



    return dbr
