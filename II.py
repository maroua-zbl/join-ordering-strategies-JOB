from pylab import *
import matplotlib.pyplot as plt
import random
import numpy as np
from queryParser import *
import psycopg2
from moz_sql_parser import parse
from moz_sql_parser import format

FACT="lineorder"
def Iterative_Improuvement(input,nsp,cursor):
    
    starting_points=[]
    locals_min=[]
    joinedTables ,parsed_query,alias=queryParser(input)
    joinedTables.remove(FACT)
    starting_points=get_starting_points(nsp,joinedTables)
    min_state=()
    #Browse the starting points
    instructions=0
    min_q=""
    for i in range(0,len(starting_points)):
        instructions+=1
        point=starting_points[i]
        query=listToQuery(point,get_indice(point),parsed_query)
        point_cost=get_energy(query,cursor)
        print('starting point',i,point,'with cost', point_cost)
        
         #initialize local_min and global_min
        local_min=point_cost
        global_min=point_cost
        
      
        neighbors=get_neighbors(point)
        for neighbor in neighbors :
            instructions+=1
            neighborQuery=listToQuery(neighbor,get_indice(neighbor),parsed_query)
            neighbor_cost=get_energy(neighborQuery,cursor)
            print('neighbor' , neighbor, 'with cost', neighbor_cost)
    
            if neighbor_cost<=local_min:
                local_min=neighbor_cost
                min_q=neighborQuery
                #print('min state',min_state)
                
        locals_min.append((min_q,local_min))
        print("\n")
                
    #browse the locals_min to get global_min and the solution     
    for s,c in locals_min:
        if(c<=global_min):
            global_min=c
            min_state=(s,c)
            
    
    return min_state,instructions


