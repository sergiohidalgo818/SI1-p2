#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from app import app
from app import database
from flask import render_template, request, url_for
import os
import sys
import time

@app.route('/', methods=['POST','GET'])
@app.route('/index', methods=['POST','GET'])
def index():
    
    return render_template('index.html')


@app.route('/borraEstado', methods=['POST','GET'])
def borraEstado():
    if 'state' in request.form:
        state    = request.form["state"]
        bSQL    = request.form["txnSQL"]
        bCommit = "bCommit" in request.form
        bFallo  = "bFallo"  in request.form
        duerme  = request.form["duerme"]
        dbr = database.delState(state, bFallo, bSQL=='1', int(duerme), bCommit)
        return render_template('borraEstado.html', dbr=dbr)
    else:
        return render_template('borraEstado.html')

    
@app.route('/topUK', methods=['POST','GET'])
def topUK():
    client = database.getMongoClient()
    mycol = database.getMongoCollection(client)
    movies=[[],[],[]]
    # TODO: consultas a MongoDB ...
    if 'boton' in request.form:
        if 'comedias' in request.form:
            movies = database.getComedymongo(mycol)
    
        if 'accion' in request.form:
            movies = database.getActionmongo(mycol)
            
    
        if 'darrenkatie' in request.form:
            movies = database.get2Actors(mycol)


    database.mongoDBCloseConnect(client)
            
    
    return render_template('topUK.html', movies=movies)