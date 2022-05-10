from pylab import *
import matplotlib.pyplot as plt
import random
import numpy as np
from queryParser import *
import psycopg2
import time


# input="SELECT SUM(l.lo_extendedprice * l.lo_discount) AS revenue FROM lineorder l , date d WHERE l.lo_orderdate = d.d_datekey AND d.d_year = 1993 AND l.lo_discount BETWEEN 1 AND 3 AND l.lo_quantity < 25;"


tablesWithSel={}
sortedTables=[]

def init():
    global tablesWithSel
    tablesWithSel={}
    global sortedTables
    sortedTables=[]
    global tablesSel
    tablesSel={}
    
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

def FanOut(input,cursor):
    init()
    # calculer les cardinalités des tables
    joinedTables ,joinedClauses,parsed_query , alias=queryParser(input)
    joinedTables.remove(FACT)
    inter=[]
    tablesSel=get_tableWithSelectivity(parsed_query)
    print('ttttttttttttttttttt',tablesSel)
    for c in joinedClauses:
        t1,t2=get_alias(c)
        print(t1,t2)
        json={'select': {'value': {'count': '*'}},'from':  [{'value': alias[t1], 'name': t1},{'left join': {'name':t2,'value':alias[t2]},                 'on': {'eq': c}}]}
       
        if (t1 in tablesSel.keys()) and (t2 in tablesSel.keys()):
            inter.extend(tablesSel[t1])
            inter.extend(tablesSel[t2])
            print(len(inter))
            json['where']={'and':inter }
        elif (t1 in tablesSel.keys()):
            print("t1" , tablesSel[t1])
            json['where']={"and":tablesSel[t1]}
        elif (t2 in tablesSel.keys()):
            print("t2" , tablesSel[t2])
            json['where']={"and":tablesSel[t2]}
        query=format(json)
        print("qqqqqqqqqqqqqq",query)
        inter=[]
        cursor.execute(query)
        tablesWithSel[t1+'.'+t2]=cursor.fetchall()[0][0]
        
           
    sortedTables={k: v for k, v in sorted(tablesWithSel.items(), key=lambda item: item[1])}
    print("sorteeeed clauses",sortedTables)
    
    return sortedTables

