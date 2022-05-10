#!/usr/bin/env python
# coding: utf-8

# In[3]:

import os
import random as random
import numpy as np
from queryParser import *
import psycopg2
import json
from moz_sql_parser import parse
from moz_sql_parser import format
joinedClauses=[]
alias={}
def init():
    
    joinedClauses=[]
    alias={}
    

def queryParser(input):
    init()
    parsed_query=parse(input)
    from_clause=parsed_query["from"]
    for j in range(0,len(from_clause)):
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
            #apply algorithm
   
            #table avec alias sans join clause from:[{}{}{}]
        
    return parsed_query ,joinedClauses , alias   
            
            ##########################################################################################



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

        
def exist(a,usedTables):
    i=0
    j=0
    position=-1
    while(i<len(usedTables)):
        while(j<len(usedTables[i]) and usedTables[i][j]!=a):
            j+=1
            
        if j==len(usedTables[i]):
            i+=1
            j=0
            
        else:
            position=i
            return position
    return position

def insert_TT_join(a1,a2,clause,result):
    p=np.random.randint(2)
    if p==0:
        result.append("("+alias[a1]+" as "+a1+" "+"join "+alias[a2]+" as "+a2+" on "+clause[0]+"="+clause[1]+")")   
    else:
        result.append("("+alias[a2]+" as "+a2+" join "+alias[a1]+" as "+a1+" on "+clause[0]+"="+clause[1]+")")
    return result

def insert_TTR_join(a,clause,result,p1):
    p=np.random.randint(2)
    if p==0:
        result[p1]="("+alias[a]+" as "+a+" "+"join "+result[p1]+" on "+clause[0]+"="+clause[1]+")"
    else:
        result[p1]="("+result[p1]+" join "+alias[a]+" as "+a+" on "+clause[0]+"="+clause[1]+")"
    return result
def insert_TRTR_join(clause,result,p1,p2):
    p=np.random.randint(2)
    if p==0:
        j="("+result[p1]+" join "+result[p2]+" on "+clause[0]+"="+clause[1]+")"
        #result = [e for e in result if e not in [result[p1],result[p2]]]
        result[p1]=j
        result.remove(result[p2])
        #result.insert(0,j)
    else:
        j="("+result[p2]+" join "+result[p1]+" on "+clause[0]+"="+clause[1]+")"
        #result = [e for e in result if e not in [result[p1],result[p2]]]
        #result.insert(0,j)
        result[p1]=j
        result.remove(result[p2])
    return result
    
def get_randomState( parsed_query,alias,clauses):
    clauses=random.sample(clauses, len(clauses))
    query=get_leftDeep(parsed_query,alias,clauses)
    return (query,clauses)
    
def get_query(parsed_query,alias,clauses2):
    clauses=[]
    clauses.extend(clauses2)
    usedTables=[]
    queryTables={}
    queryTables.update(alias)
    result=[]
    t=[]
    new_json=parsed_query
    #clauses=random.sample(joinedClauses, len(joinedClauses))
    #print('state',clauses)
    a1,a2=get_alias(clauses[0])
    result=insert_TT_join(a1,a2,clauses[0],result)
    usedTables.append([a1,a2])
    #print(new_from)
    clauses.remove(clauses[0])
    del queryTables[a1]
    del queryTables[a2]
    #print("len querytables",len(queryTables))
    while(len(queryTables)>0):
        i=0
        
        #print("result",result)
        #print("usedtables",usedTables)
        while(i<len(clauses)) :
            
            a1,a2=get_alias(clauses[i])
            #print(a1,a2)
            p1=exist(a1,usedTables)
            p2=exist(a2,usedTables)
            #print(p1,p2)
            #print("len querytables",len(queryTables))
            if(p1==-1)and(p2==-1):
                
                result=insert_TT_join(a1,a2,clauses[i],result)
                usedTables.append([a1,a2])
                del queryTables[a1]
                del queryTables[a2]
                clauses.remove(clauses[i])
                #print("len querytables",len(queryTables))
                
            elif(p1!=-1 and p2==-1):
                
                
                result=insert_TTR_join(a2,clauses[i],result,p1)
                usedTables[p1].append(a2)
                del queryTables[a2]
                clauses.remove(clauses[i])
                #print("len querytables",len(queryTables))
            elif(p2!=-1 and p1==-1):
                
                
                result=insert_TTR_join(a1,clauses[i],result,p2)
                usedTables[p2].append(a1)
                del queryTables[a1]
                clauses.remove(clauses[i])
                #print("len querytables",len(queryTables))
            elif(p1!=-1 and p2!=-1 and p1!=p2):
            
                result=insert_TRTR_join(clauses[i],result,p1,p2)
                #print("ggggg",usedTables[p1])
                #print("ggggg",usedTables[p2])
                #r=usedTables[p2]
                usedTables[p1].extend(usedTables[p2])
                #print("tttttttttttttt",usedTables[p1].extend(usedTables[p2]))
                #new=usedTables[p1]
                #usedTables.remove(usedTables[p1])
                usedTables.remove(usedTables[p2])
                #usedTables.insert(0,t)
                #print("hhhhhhhhhhh",usedTables)
            
                #usedTables = [e for e in usedTables if e not in [usedTables[p1],usedTables[p2]]]
                clauses.remove(clauses[i])  
                
            else:
                
                i+=1
            #print("result",result)
            #print("usedtables",usedTables)
            
    
    select=format({'select':parsed_query['select']})
    where_clause=new_json["where"]["and"]
    k=0
    while k<len(where_clause) :
       
        if "eq" in where_clause[k]:
            if isinstance(where_clause[k]['eq'][1],str):
                where_clause.remove(where_clause[k])   
            else:
                k+=1
        else:
            k+=1
    
    where=format({'where':{'and':where_clause}})
    query=select+" from "+result[0]+where
    
    return query
                                 


        
def exist2(a1,a2,usedTables):
    if(t_exist(a1,usedTables)and t_exist(a2,usedTables)):
        return []
    elif (t_exist(a1,usedTables)):
        return [a2]
    elif (t_exist(a2,usedTables)):
        return [a1]
    else:
        return [a1,a2]
def get_leftDeep(parsed_query,alias,clauses2):
    
    usedTables=[]
    queryTables={}
    queryTables.update(alias)
    clauses=[]
    clauses.extend(clauses2)
    #print('qieryTables',queryTables)
    new_json=parsed_query
    #clauses=random.sample(Clauses2, len(Clauses2))
    #print('state',clauses)
    a1,a2=get_alias(clauses[0])
    #print(a1,a2)
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
            #print(a1,a2)
            #print('used table',usedTables)
            c=exist2(a1,a2,usedTables)
            #print('cc',c)
            if(len(c)==1):
                #print('esss')
                new_from.append({'join':{'name':c[0],'value':alias[c[0]]}, 'on':{'eq':clauses[i]}})
                usedTables.append(c[0])
                del queryTables[c[0]]
                #print('queryTables',len(queryTables))
                clauses.remove(clauses[i])
            elif(len(c)==0):
                clauses.remove(clauses[i])
                
            
            i+=1
            
            
            
    
    new_json["from"]=new_from
    where_clause=new_json["where"]["and"]
    for t in where_clause :
        if "eq" in t:
            if isinstance(t['eq'][1],str):
                joinedClauses.append(t['eq'])
                del(t)
    new_query=format(new_json)
    
    return new_query
                                 





