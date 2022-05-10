#!/usr/bin/env python
# coding: utf-8

# In[1]:


import threading
import time
import os,sys
from yoctopuce.yocto_api import *
from yoctopuce.yocto_temperature import *
from yoctopuce.yocto_power import *
import csv
#from functions import *
class save(threading.Thread):
    def __init__(self, nom = ''):
        threading.Thread.__init__(self)
        self.nom = nom
        self.Terminated = False
    def run(self):
        file = open('C:/Users/zeblahm/Desktop/GA1.csv', "w+")
        errmsg = YRefParam()
        #sensor = YPower.FirstPower()
        if YAPI.RegisterHub("usb", errmsg) != YAPI.SUCCESS:
            sys.exit("init error" + errmsg.value)

        # On récupère l'objet permettant d'intéragir avec le module
        power = YPower.FindPower('YWATTMK1-18622A.power')
        while not self.Terminated:
    
            
            p=power.get_currentValue()
            #print(p)
            line=str(p)+","+str(power.get_cosPhi())+"\n"
            file.write(line)
            #print("line",line)
            YAPI.Sleep(1)
        
        file.close()
        YAPI.FreeAPI()
    def stop(self):
        self.Terminated = True
        #sys.exit("exit")
        #file.close()
        #print('enfin') 
    def get_initialePower(self):
        errmsg = YRefParam()
        #sensor = YPower.FirstPower()
        if YAPI.RegisterHub("usb", errmsg) != YAPI.SUCCESS:
            sys.exit("init error" + errmsg.value)

        # On récupère l'objet permettant d'intéragir avec le module
        power = YPower.FindPower('YWATTMK1-18622A.power')
        
        
        return power.get_currentValue()*power.get_cosPhi()
        
        

        



