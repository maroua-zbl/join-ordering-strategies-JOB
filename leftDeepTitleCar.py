import random
import numpy as np
from queryParser import queryParser,listToQuery
from functions import *
import psycopg2
from moz_sql_parser import parse
from moz_sql_parser import format
from FanOutCar import *
joinedClauses=[]
alias={}
def init():
    
    joinedClauses=[]
    alias={}

def get_joinedClause(t,joinedClauses):
    
    i=0
    while i<len(joinedClauses):
        c=joinedClauses[i]
        r1=c[0].rpartition('.')[0]
        r2=c[1].rpartition('.')[0]
        if((t==r1)and('t'==r2))or((t==r2)and('t'==r1)):
            return c
        else:
            i+=1  
    return ''


def get_alias(clause):
    a1=clause[0].rpartition('.')[0]
    a2=clause[1].rpartition('.')[0]
    return a1,a2


def t_exist(t,table):
    i=0
    while i<len(table) and table[i]!=t:
        i+=1
    if i==len(table) :
        return False
    else:
        return True

        
def exist(a1,a2,usedTables):
    if(t_exist(a1,usedTables)and t_exist(a2,usedTables)):
        return []
    elif (t_exist(a1,usedTables)):
        return [a2]
    elif (t_exist(a2,usedTables)):
        return [a1]
    else:
        return [a1,a2]
    
def get_joinedClause2(t1,t2,joinedClauses):
    
    i=0
    while i<len(joinedClauses):
        c=joinedClauses[i]
        r1=c[0].rpartition('.')[0]
        r2=c[1].rpartition('.')[0]
        if((t1==r1)and(t2==r2))or((t1==r2)and(t2==r1)):
            return c
        else:
            i+=1  
    return ''    
def trierClauses(input,cursor,joinedClauses):
    result=[]
    clauses=[]
    clauses.extend(joinedClauses)
    fanout,cardinalities=FanOut(input,cursor)
    fanout=list(fanout.keys())
    cardinalities=list(cardinalities.keys())
    
    for t in fanout:
        c=get_joinedClause(t,joinedClauses)
        if len(c)>0:
            result.append(c)
            clauses.remove(c)
        
    for i in range(0,len(cardinalities)-1):
        print("iiiiiiiii",i)
        for j in range(i+1,len(cardinalities)):
            print("jjjjjjjj",j)
            c=get_joinedClause2(cardinalities[i],cardinalities[j],joinedClauses)
            if len(c)>0:
                result.append(c)
                clauses.remove(c)
            
    result.extend(clauses)                
    print(result)  
    return result
            
        
def get_randomState(input,cursor):
    joinedTables ,joinedClauses,parsed_query , alias=queryParser(input)
    usedTables=[]
    queryTables={}
    # extend
    queryTables.update(alias)
    #print('qieryTables',queryTables)
    new_json=parsed_query
    clauses=trierClauses(input,cursor,joinedClauses)
    a1,a2=get_alias(clauses[0])
    
    p=np.random.randint(2)
    if p==0:
        new_from=[{"value":alias[a1],"name":a1},{'join':{'name':a2,'value':alias[a2]} , 'on':{'eq':clauses[0]}  }]
        
    else:
        new_from=[{"value":alias[a2],"name":a2},{'join':{'name':a1,'value':alias[a1]}, 'on':{'eq':clauses[0]}   }]
    usedTables.append(a1)
    usedTables.append(a2)
    #print(new_from)
    clauses.remove(clauses[0])
    del queryTables[a1]
    del queryTables[a2]
    #print('queryTables',len(queryTables))
    
    while(len(queryTables)>0):
        i=0
        
        while(i<len(clauses)) :
            
            
            a1,a2=get_alias(clauses[i])
           
            c=exist(a1,a2,usedTables)
            
            if(len(c)==1):
                
                new_from.append({'join':{'name':c[0],'value':alias[c[0]]}, 'on':{'eq':clauses[i]}})
                usedTables.append(c[0])
                del queryTables[c[0]]
              
                clauses.remove(clauses[i])
            elif(len(c)==0):
                clauses.remove(clauses[i])
                
            else:
                i+=1
            
            
            
    
    new_json["from"]=new_from
    where_clause=new_json["where"]["and"]
    for t in where_clause :
        if "eq" in t:
            if isinstance(t['eq'][1],str):
                joinedClauses.append(t['eq'])
                del(t)
    new_query=format(new_json)
    cost=get_runTime(new_query,cursor)
    return [new_query,cost]
                                 
                                 
                
                
            
    
    
    