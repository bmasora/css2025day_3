# -*- coding: utf-8 -*-
"""
Created on Thu Jan 30 13:36:03 2025

@author: UJ
"""

import os
import requests as req

try:
    os.mkdir("bioact")
except FileExistsError:
    pass

res = req.get('https://phytochem.nal.usda.gov/biological-activities-chemicals-csv-export/4/all?page&_format=csv')
# https://phytochem.nal.usda.gov/biological-activities-chemicals-csv-export/5/all?page&_format=csv

csv = open("bioact/4.csv", "wb")
csv.write(res.content)
csv.close()
print("File 4 downloded")



#to download more that one file

#res = req.get('https://phytochem.nal.usda.gov/biological-activities-chemicals-csv-export'+str(bio)+'/all?page&_format=csv')
# https://phytochem.nal.usda.gov/biological-activities-chemicals-csv-export/5/all?page&_format=csv



for bio in range(1, 6):
    if os.path.isfile("https://phytochem.nal.usda.gov/biological-activities-chemicals-csv-export"+str(bio)+"/all?page&_format=csv"):
        print(str(bio)+" already downloaded")
    else:
        res = req.get("https://phytochem.nal.usda.gov/biological-activities-chemicals-csv-export"+str(bio)+"/all?page&_format=csv")
        csv = open("bioact/"+str(bio)+".csv", "wb")
        csv.write(res.content)
        csv.close()
        print("Downloading "+str(bio))
        
import pubchempy as pcp
import pandas as pd
import glob
from pandas.errors import EmptyDataError

try:
    os.mkdir("SDFS")
except FileExistsError:
    pass

csv_files = {}
csv_files = glob.glob("bioact/*.csv")

lig = []

for files in csv_files:
    try:
        temp_df = pd.read_csv(files, sep=",")
        lig.append(temp_df)
    except EmptyDataError():
        print(files+"is an empty csv file")
        
#print(lig)
df = pd.concat(lig, axis=0, ignore_index=True)
#print(df)

for name in df["Chemical Name"]:
    compounds = pcp.get_compounds(name, "name")
    for compound in compounds:
        cid = compound.cid
        pcp.download("SDF", "SDFS"+str(cid)+",sdf", cid, "cid")
        print("Downloading" + str(cid))


