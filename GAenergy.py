

from pylab import *
import matplotlib.pyplot as plt
import random as r
import numpy as np
#from queryParser import *
import psycopg2
from moz_sql_parser import parse
from moz_sql_parser import format
#from functions import *
from bushyTreeGA import *
# Enter here the percent of top-grated individuals to be retained for the next generation (range 0-1)
GRADED_RETAIN_PERCENT = 0.5
 
# Enter here the chance for a non top-grated individual to be retained for the next generation (range 0-1)
CHANCE_RETAIN_NONGRATED = 0.2
 
# Number of individual in the population
POPULATION_COUNT = 150
 
# Maximum number of generation before stopping the script
GENERATION_COUNT_MAX = 100
 
#Number of top-grated individuals to be retained for the next generation
GRADED_INDIVIDUAL_RETAIN_COUNT = int(POPULATION_COUNT * GRADED_RETAIN_PERCENT)


costs=[]

def set_GENERATION_COUNT_MAX(val):
    global GENERATION_COUNT_MAX
    GENERATION_COUNT_MAX=val
    
def get_random_population(parsed_query,alias,joinedClauses):
    """ Create a new random population, made of `POPULATION_COUNT` individual. """
    return [get_randomState(parsed_query,alias,joinedClauses) for _ in range(POPULATION_COUNT)]

 
""""def average_population_cost(population,parsed_query,alias):
    Return the average fitness of all individual in the population. 
    total = 0
    for individual in population:
        total += get_cost(get_query(parsed_query,aliasindividual))
    return total / POPULATION_COUNT"""
  
def grade_population(population,parsed_query,alias,cursor):
    global costs
    """ Grade the population. Return a list of tuple (individual, fitness) sorted from most graded to less graded. """
    graded_individual = []
    i=[]
    ii=[]
    for t in range(0,len(population)):
        #print("item", t,"\n")
        i.extend(population[t])
        ii.extend(population[t])
        
        graded_individual.append((i, get_energy(ii[0],cursor)))
        #print("individual",graded_individual[len(graded_individual)-1][0] ,"cost",graded_individual[len(graded_individual)-1][1],"\n\n")
        costs.append(graded_individual[len(graded_individual)-1][1])
        i=[]
        ii=[]
    
    return sorted(graded_individual, key=lambda x: x[1], reverse=False)



def get_local_min(sorted_population):
    min=100000000
    for state,cost in sorted_population:
        if cost<min :
            min=cost
            
    return min    



def evolve_population(sorted_population,parsed_query,alias):
    """ Make the given population evolving to his next generation. """
  
    # Get individuals sorted by grade (top first), the average cost 
    #average_cost = 0
    graded_population = []
    for individual, fitness in sorted_population:
        #average_cost += fitness
        #print("leeeeeeeeen ind",len(individual))
        graded_population.append(individual)
    #average_cost /= POPULATION_COUNT
        
    # Filter the top graded individuals
    parents = graded_population[:GRADED_INDIVIDUAL_RETAIN_COUNT]
   
 
    # Randomly add other individuals to promote genetic diversity
    for individual in graded_population[GRADED_INDIVIDUAL_RETAIN_COUNT:]:
        if r.uniform(0, 1) < CHANCE_RETAIN_NONGRATED:
            parents.append(individual)
    
   
    # Crossover parents to create children
    #ind_len=len(parents[1])
    parents_len = len(parents)
    #print("len parents",parents_len)
    desired_len = POPULATION_COUNT - parents_len
    children = []
    #print("parents",parents)
    while len(children) < desired_len:
        parent=random.sample(parents, 1)[0]
        #print("parent",parent)
        child_clauses=move(parent[1])
        child_query=get_leftDeep(parsed_query,alias,child_clauses)
        child=(child_query,child_clauses)
        #print("child",child,'\n')
        children.append(child)
    
    # The next generation is ready
    parents.extend(children)

    return parents

def Genetic_algorithm(input,cursor):
    joinedClauses=[]
    parsed_query,joinedClauses,alias=queryParser(input)
    #generate the initial population
    population = get_random_population(parsed_query,alias,joinedClauses)
    #print('Initial population',population[2],'\n')
    
    i = 0
    #costs_avg = []
    locals_min=[]
    #Average cost of the initial population
    #average_cost = average_population_cost(population)
    #costs_avg.append(average_cost)
    #print("average cost",costs_avg,'\n')

    # Make the population evolve
    while  i < GENERATION_COUNT_MAX:
        #print("iteration ", i)
       
        sorted_population=grade_population(population,parsed_query,alias,cursor)
        
      
        population = evolve_population(sorted_population,parsed_query,alias)
     
        i += 1
     
        

    min_state=grade_population(population,parsed_query,alias,cursor)[0]
    return min_state

 

   
