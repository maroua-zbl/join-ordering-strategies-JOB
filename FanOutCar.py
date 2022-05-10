
from pylab import *
import matplotlib.pyplot as plt
import random
import numpy as np
from queryParser import *
import psycopg2
import time


# input="SELECT SUM(l.lo_extendedprice * l.lo_discount) AS revenue FROM lineorder l , date d WHERE l.lo_orderdate = d.d_datekey AND d.d_year = 1993 AND l.lo_discount BETWEEN 1 AND 3 AND l.lo_quantity < 25;"


fanOut={}
cardinalities={}
sortedTables=[]
FACT="title"
def init():
    global tablesWithSel
    tablesWithSel={}
    global sortedTables
    sortedTables=[]
    
def get_titleJoinTables(joinedClauses):
    result=[]
    for c in joinedClauses:
        a1,a2=get_alias(c)
        
        if(a1=='t'):
            result.append(a2)
        elif (a2=='t'):
            result.append(a1)
    return result

def FanOut(input,cursor):
    init()
    state=[]
    solution={}
    titleTablesSel=[]
    
    # calculer les cardinalités des tables
    joinedTables ,joinedClauses,parsed_query , alias=queryParser(input)
    joinedTables.remove(FACT)
    print('joined tables' ,joinedTables)
    tablesSel=get_tableWithSelectivity(parsed_query)

    if 't'in tablesSel:
        
        tSel=tablesSel['t']
        query="select count(*) from title t " + format({"where":{"and":tSel}})
        print("query",query)
        cursor.execute(query)
        factCar=cursor.fetchall()[0][0] 
    else :
        tSel=[]
        query="select count(*) from title t " 
        print("query",query)
        cursor.execute(query)
        factCar=cursor.fetchall()[0][0]
    
   
 
        
    titleTables=get_titleJoinTables(joinedClauses)
  



    titleTablesSel= {key: tablesSel[key] for key in set(titleTables).intersection(tablesSel)}
    
            
    for key in titleTablesSel.keys():
        json={'select': {'value': {'count': '*'}},'from':  [{'value': 'title', 'name': 't'},{'left join': {'name':key,'value':alias[key]},                 'on': {'eq': get_joinedClause(alias[key])}}]}
        if len(tSel)==0:
            if len(titleTablesSel[key])==1:
                json['where']=tablesSel[key][0]
            else:
                json['where']={'and':tablesSel[key]}
            query=format(json)
            print("query",query)
            cursor.execute(query)
            fanOut[key]=cursor.fetchall()[0][0]/factCar
        else:
            tablesSel[key].extend(tSel)
            json['where']={'and':tablesSel[key]}
            query=format(json)
            print("query",query)
            cursor.execute(query)
            fanOut[key]=cursor.fetchall()[0][0]/factCar
            
    if(len(titleTables)>len(tablesWithSel)):
        for t in titleTables:
            if t not in tablesWithSel:
                if len(tSel)==0:
                    query="select count(*) from title t left join "+alias[t]+" as "+t+" on "+get_joinedClause(alias[t])[0]+" = "+get_joinedClause(alias[t])[1] 
                else :
                    query="select count(*) from title t left join "+alias[t]+" as "+t+" on "+get_joinedClause(alias[t])[0]+" = "+get_joinedClause(alias[t])[1] +" "+ format({" where ":tSel})
                    
                print("query",query)
                cursor.execute(query)
                fanOut[t]=cursor.fetchall()[0][0]/factCar
                
    fanout={k: v for k, v in sorted(fanOut.items(), key=lambda item: item[1])}
    #calculer les cardinalites des tables restantes
    if(len(joinedTables)>len(fanOut)):
        for t in joinedTables:
            if t not in fanout :
                if t not in tablesSel:
                    query="select count(*) from " + t
                    print("query",query)
                    cursor.execute(query)
                    cardinalities[get_key(t,alias)]=cursor.fetchall()[0][0]
                else:
                    query="select count(*) from " + t + format({"where":{"and":tablesSel[t]}})
                    print("query",query)
                    cursor.execute(query)
                    cardinalities[get_key(t,alias)]=cursor.fetchall()[0][0]
  
    print("\n"+"FanOut",fanout)
    return fanout, cardinalities


