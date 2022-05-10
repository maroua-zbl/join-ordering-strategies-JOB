from pylab import *
import matplotlib.pyplot as plt
import random as r
import numpy as np
from queryParser import queryParser,listToQuery
import psycopg2
import time
from moz_sql_parser import parse
from moz_sql_parser import format
#from functions import *
from bushyTreeGA import *


    
TEMPERATURE = 250
TEMP_REDUCTION_FACTOR = 0.1
STEPS_TO_EQUILIBRIUM = 300

def set_temperature(v):
    global TEMPERATURE
    TEMPERATURE=v
    
def acc_probality(old,new):
    # probality to accept the new state even if new_cost>old_cost
    global TEMPERATURE
    return np.exp( (old - new) /TEMPERATURE )

def is_acceptable(new_cost,old_cost):
    p = r.random()
    if new_cost<old_cost:
        return True
    elif acc_probality(old_cost, new_cost) > p:
        print("acceeeeeeeeeeeeeeeeeeeeeeptaaaaaaaaaaaaaaableeeeeeeeeeeeeeeeeee")
        return True
    else:
        return False    
        
def reduce_temperature():
    global TEMPERATURE
    TEMPERATURE *= TEMP_REDUCTION_FACTOR
    
def is_equilibrium():
    global STEPS_TO_EQUILIBRIUM
    if STEPS_TO_EQUILIBRIUM == 0:
        STEPS_TO_EQUILIBRIUM = 300
        return True
    else:
        STEPS_TO_EQUILIBRIUM -= 1
        return False

def is_frozen():
    if TEMPERATURE < 0.1:
        return True
    else:
        return False
     
def simulated_annealing(input,cursor,old_state):
    parsed_query,joinedClauses,alias=queryParser(input)
    print("t: ", TEMPERATURE)
    #variables initialization
    costs=[]
    locals_min=[]
    
    #old_state=get_randomState(parsed_query,alias,joinedClauses)
    old_cost=get_cost(old_state[0],cursor)
    costs.append(old_cost)
    global_min=old_cost
    local_min=old_cost
    min_state=(old_state,old_cost)
    #print("min state",min_state)
    
    i=0
    j=0
    k=1
    iterations=0
    while not is_frozen() :
        
        while not is_equilibrium():
                iterations+=1
                print("iteration ",k,"\n")
                #get new state and new cost 
                new_state_clauses=move(old_state[1])
                new_state=(get_query(parsed_query,alias,new_state_clauses),new_state_clauses)
                new_cost= get_cost(new_state[0],cursor)
                #print(new_state," with execution time ",new_cost,"\n")
                #print("new state :",new_state, 'with cost :', new_cost)
                costs.append(new_cost)
                if (is_acceptable(new_cost,old_cost)):
                    old_state=new_state
                 #update global_min
                    if new_cost<global_min:
                        global_min=new_cost
                        min_state=(new_state,new_cost)
                        
                    if new_cost<local_min:
                        local_min=new_cost
                        j=0
                    else :
                        j+=1
                        if j==1 :
                            locals_min.append((old_state,local_min))
                
                local_min=new_cost 
                k+=1
        reduce_temperature()
        
    
    
    
    
    return min_state
    
    

   
 

   
