#!/usr/bin/env python
# coding: utf-8

# In[3]:


import random
import numpy as np
from queryParser import *
#from functions import *
import psycopg2
from moz_sql_parser import parse
from moz_sql_parser import format
from FanOutAll import *
joinedClauses=[]
alias={}
def init():
    
    joinedClauses=[]
    alias={}
    

        
            
            
            ##########################################################################################

def alias():
    global alias
    alias=get_alias_queryParser()
    
    

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

        
def exist2(a1,a2,usedTables):
    if(t_exist(a1,usedTables)and t_exist(a2,usedTables)):
        return []
    elif (t_exist(a1,usedTables)):
        return [a2]
    elif (t_exist(a2,usedTables)):
        return [a1]
    else:
        return [a1,a2]
    
def insert_TT_join(a1,a2,clause,result,alias):
    
    p=np.random.randint(2)
    if p==0:
        print(alias[a1],alias[a2])
        result.append("("+alias[a1]+" as "+a1+" "+"join "+alias[a2]+" as "+a2+" on "+clause[0]+"="+clause[1]+")")   
    else:
        print(alias[a1],alias[a2])
        result.append("("+alias[a2]+" as "+a2+" join "+alias[a1]+" as "+a1+" on "+clause[0]+"="+clause[1]+")")
    return result

def insert_TTR_join(a,clause,result,alias,p1):
    p=np.random.randint(2)
    if p==0:
        result[p1]="("+alias[a]+" as "+a+" "+"join "+result[p1]+" on "+clause[0]+"="+clause[1]+")"
    else:
        result[p1]="("+result[p1]+" join "+alias[a]+" as "+a+" on "+clause[0]+"="+clause[1]+")"
    return result
def insert_TRTR_join(clause,result,alias,p1,p2):
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

def get_joinedClause(t1,t2,joinedClauses):
    
    i=0
    while i<len(joinedClauses):
        c=joinedClauses[i]
        r1=c[0].rpartition('.')[0]
        r2=c[1].rpartition('.')[0]
        if((t1==r1)and(t2==r2))or((t1==r2)and(t2==r1)):
            return c
        else:
            i+=1  
    return []
def trierClauses(input,cursor,joinedClauses):
    result=[]
    clauses=[]
    clauses.extend(joinedClauses)
    
    fanout=FanOut(input,cursor).keys()
    print("faaaaaaaaaaaaaanout" , fanout)
    for f in fanout:
        t1=f.rpartition('.')[0]
        t2=f.rpartition('.')[2]
        c=get_joinedClause(t1,t2,joinedClauses)
        result.append(c)    
     
    return result    
    
    
def get_BushyTree(input,cursor):
    joinedTables,joinedClauses ,parsed_query,alias=queryParser(input)
    usedTables=[]
    queryTables={}
    queryTables.update(alias)
    result=[]
    t=[]
    new_json=parsed_query
    
    
    clauses=trierClauses(input,cursor,joinedClauses)
    
    a1,a2=get_alias(clauses[0])
    result=insert_TT_join(a1,a2,clauses[0],result,alias)
    print("resuuuuuult",result)
    usedTables.append([a1,a2])
    #print(new_from)
    clauses.remove(clauses[0])
    del queryTables[a1]
    del queryTables[a2]
    
    
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
            
            if(p1==-1)and(p2==-1):
                
                result=insert_TT_join(a1,a2,clauses[i],result,alias)
                usedTables.append([a1,a2])
                del queryTables[a1]
                del queryTables[a2]
                clauses.remove(clauses[i])
                
            elif(p1!=-1 and p2==-1):
                
                
                result=insert_TTR_join(a2,clauses[i],result,alias,p1)
                usedTables[p1].append(a2)
                del queryTables[a2]
                clauses.remove(clauses[i])
                
            elif(p2!=-1 and p1==-1):
                
                
                result=insert_TTR_join(a1,clauses[i],result,alias,p2)
                usedTables[p2].append(a1)
                del queryTables[a1]
                clauses.remove(clauses[i])
                
            elif(p1!=-1 and p2!=-1 and p1!=p2):
            
                result=insert_TRTR_join(clauses[i],result,alias,p1,p2)
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
    print(type(query))
    cost=get_runTime(query,cursor)
    return [query,cost]
                                 
                                 
def get_leftDeep(input,cursor):                
    joinedTables ,joinedClauses,parsed_query , alias=queryParser(input)
    usedTables=[]
    queryTables={}
    # extend
    queryTables.update(alias)
    #print('qieryTables',queryTables)
    new_json=parsed_query
    clauses=trierClauses(input,cursor,joinedClauses)
    print('state',clauses)
    a1,a2=get_alias(clauses[0])
    print(a1,a2)
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
            print('iii',i)
            
            a1,a2=get_alias(clauses[i])
            print(a1,a2)
            print('used table',usedTables)
            c=exist2(a1,a2,usedTables)
            print('cc',c)
            if(len(c)==1):
                print('esss')
                new_from.append({'join':{'name':c[0],'value':alias[c[0]]}, 'on':{'eq':clauses[i]}})
                usedTables.append(c[0])
                del queryTables[c[0]]
                print('queryTables',len(queryTables))
                clauses.remove(clauses[i])
            elif(len(c)==0):
                clauses.remove(clauses[i])
                
            else:
                i+=1
            print("clauseeeees",clauses)
            
            
    
    new_json["from"]=new_from
    where_clause=new_json["where"]["and"]
    for t in where_clause :
        if "eq" in t:
            if isinstance(t['eq'][1],str):
                joinedClauses.append(t['eq'])
                del(t)
    new_query=format(new_json)
    return [new_query,cost]
    
         


    
    








