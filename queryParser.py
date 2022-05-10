#!/usr/bin/env python
# coding: utf-8

# In[3]:
import re
import random
import numpy as np
import psycopg2
import json
from moz_sql_parser import parse
from moz_sql_parser import format
from save import *

#SELECT * FROM songplays,songs,times where  songplays.song_id = songs.song_id and times.start_time=songplays.start_time   
#SELECT * FROM songplays as sp,songs as s,times as t where  sp.song_id = s.song_id and t.start_time=sp.start_time
#SELECT * FROM songplays join songs on songplays.song_id = songs.song_id join times on times.start_time=songplays.start_time
#SELECT * FROM songplays as sp  join songs as s on s.song_id = sp.song_id JOIN  times as t ON t.start_time = sp.start_time

FACT="title"

joinedTables=[]
joinedClauses=[]
alias={}

def init():
    global joinedTables
    joinedTables=[]
    global joinedClauses
    joinedClauses=[]
    global alias
    alias={}
    
def get_alias_queryParser():
    global alias
    return alias

def get_alias(clause):
    a1=clause[0].rpartition('.')[0]
    a2=clause[1].rpartition('.')[0]
    return a1,a2

def get_joinedClause(t):
    global joinedClauses
    global alias
    i=0
    t1=get_key(t,alias)
    t2=get_key(FACT,alias)
 
    while i<len(joinedClauses):
        c=joinedClauses[i]
        r1=c[0].rpartition('.')[0]
        r2=c[1].rpartition('.')[0]
        if((t1==r1)and(t2==r2))or((t1==r2)and(t2==r1)):
            return c
        else:
            i+=1  
        
def queryParser(input):
    init()
    
    parsed_query=parse(input)
    from_clause=parsed_query["from"]
    
    for j in range(0,len(from_clause)):
        joinedTables.append(from_clause[j]["value"])
        alias[from_clause[j]["name"]]=from_clause[j]["value"]
                
    
    where_clause=parsed_query["where"]["and"]
    k=0
    while k<len(where_clause) :
       
        if "eq" in where_clause[k]:
            if isinstance(where_clause[k]['eq'][1],str):
                joinedClauses.append(where_clause[k]['eq'])
                where_clause.remove(where_clause[k])   
            else:
                k+=1
        else:
            k+=1

          
    return joinedTables,joinedClauses ,parsed_query,alias

            
def listToQuery(state,indice,parsed_query):
    new_from=[{"value":state[0],"name":get_key(state[0],alias) }]
    new_json=parsed_query
                
    for i in range (1,len(state)):
        new_from.append({'join':{'name':get_key(state[i],alias),'value':state[i]}})
        if(i==indice):
            new_from[i]["on"]={"eq": get_joinedClause(state[i-1])}
            
        else:
            new_from[i]["on"]={"eq":get_joinedClause(state[i])}
          
    new_json["from"]=new_from
        
    new_query=format(new_json)
             
    new_query=format(new_json)
    return new_query
          

def get_indice(state):
    k=0
    while k<len(state):
        if(state[k]==FACT):
            return k   
        else:
            k+=1

            
def get_runTime(query,cursor):
    cursor.execute("explain analyse "+ query)
    t=cursor.fetchall()
    text=t[len(t)-1][0]
    start = text.index(':')
    end = text.index('m',start+1)
    substring = text[start+1:end]
    runTime=float(substring)
    return runTime/1000

def get_cost(query,cursor):
    cursor.execute("explain (format json) "+query)
    file=cursor.fetchone()[0][0]
    result=file['Plan']["Total Cost"]
    return result

def read_csv(p0,duree):
    #print("duree",duree)
    csvf = open('C:/Users/zeblahm/Desktop/GA1.csv', "r")
    csvreader = csv.reader(csvf)
    
    somme=0
  
    for row in csvreader:
        diff=(float(row[0])*float(row[1]))-p0
        somme+=diff
        
    return somme*duree 

def get_energy(query,cursor):
    time.sleep(10)
    a = save('Thread A')
    p0=a.get_initialePower()
    print("initiale power",p0)
    a.start()
    cursor.execute("explain analyse "+query)
    a.stop()
    
    t=cursor.fetchall()
    text=t[len(t)-1][0]
    start = text.index(':')
    end = text.index('m',start+1)
    substring = text[start+1:end]
    duree=float(substring)/1000
    #print("exe",duree,"s", "\n")
    time.sleep(5)
    
    result=read_csv(p0,duree)
  
    return result

def connect_bdd(name):
    conn = psycopg2.connect(host="localhost",
                               user="postgres", password="123", 
                               database=name)
    cursor = conn.cursor()
    return [conn,cursor]


def disconnect_bdd(conn):
    conn.close()


def get_tableWithSelectivity(parsed_query):
    result={}
    where_clause=parsed_query["where"]["and"]
    for k in range(0,len(where_clause)) :
        if "or" in where_clause[k]:
                
                table=list(list(where_clause[k].values())[0][0].values())[0][0].rpartition('.')[0]
                if(table not in result):
                    result[table]=[where_clause[k]]
                else:
                    result[table].append(where_clause[k])
     
        else:
            table=list(where_clause[k].values())[0][0].rpartition('.')[0]
            if(table not in result):
                result[table]=[where_clause[k]]
            else:
                result[table].append(where_clause[k])
            
            
           
    return result

def get_key(val,dict):
    for key, value in dict.items():
         if val == value:
            return key

def sorted_alphanumeric(data):
    convert = lambda text: int(text) if text.isdigit() else text.lower()
    alphanum_key = lambda key: [ convert(c) for c in re.split('([0-9]+)', key) ] 
    return sorted(data, key=alphanum_key)
def get_sqlFiles():
    result=[]
    directory = 'C:/Users\zeblahm\Desktop\JOB\query'
    files=[]  
    
    for filename in sorted(os.listdir(directory)): 
        f = os.path.join(directory, filename) 
        files.append(f)
    
    
    result=sorted_alphanumeric(files)
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
def get_starting_points(number,R):
    starting_points=[]
    for i in range(0,number):
        starting_points.append(get_random_state(R))
    return starting_points
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
def get_indice(state):
    k=0
    while k<len(state):
        if(state[k]==FACT):
            return k   
        else:
            k+=1
            