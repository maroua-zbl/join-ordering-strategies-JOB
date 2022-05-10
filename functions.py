#!/usr/bin/env python
# coding: utf-8

# In[3]:

import random
import numpy as np
import psycopg2
import json

from queryParser import queryParser,listToQuery

FACT="lineorder"


def get_random_state(Relations):
    #Create a new random individual
    point=random.sample(Relations, len(Relations))
    p=np.random.randint(2)
    point.insert(p,FACT)
    return point 

def move(state):
    # get a new state after a move
    result=[]
    result.extend(state)
    p1=np.random.randint(len(state))
    p2=np.random.randint(len(state))
    while (not ((state[p1]==FACT and p2<2) or (state[p2]==FACT and p1<2) or (state[p1]!=FACT and state[p2]!=FACT))) or (p1==p2) :
        p1=np.random.randint(len(state))
        p2=np.random.randint(len(state))
       
    
    m=result[p1]
    result[p1]=result[p2]
    result[p2]=m
    return result

def number_neighbors(state):
    result=1
    n=len(state)-1
    for i in range(1, n):
        result+=n-i
    return result


def get_neighbors(state):
    neighbors=set()
    while(len(neighbors)<number_neighbors(state)):
        neighbor=move(state)
        #print(voisin)
        neighbors.add(tuple(neighbor))
    return neighbors

def delete_fact(R):
    k=0
    while k<len(R):
        if(R[k]==FACT):
            del R[k]
            return R
        else:
            k+=1
        
def get_indice(state):
    k=0
    while k<len(state):
        if(state[k]==FACT):
            return k   
        else:
            k+=1
            
            
def get_starting_points(number,R):
    starting_points=[]
    for i in range(0,number):
        starting_points.append(get_random_state(R))
    return starting_points
    
    
def connect_bdd(name):
    conn = psycopg2.connect(host="localhost",
                               user="postgres", password="123", 
                               database=name)
    cursor = conn.cursor()
    return [conn,cursor]

def disconnect_bdd(conn):
    conn.close()
    
def get_cost(state,mode,parsed_query,cursor):
    
    query=listToQuery(state,get_indice(state),mode,parsed_query)
    #print("queryyyyyyyyyyy",query)
    cursor.execute("explain (format json) "+query)
    file=cursor.fetchone()[0][0]
    result=file['Plan']["Total Cost"]
    
    return result
       
def postgres_cost(query,cursor):
    cursor.execute("explain (format json) "+query)
    file=cursor.fetchone()[0][0]
    return file['Plan']["Total Cost"]    