import requests as req
import os

# Create a folder for the bioact
try:
    os.mkdir("bioact")
except FileExistsError:
    pass

# Define the URL of the .gov webpage
base = 'https://phytochem.nal.usda.gov'

# Change range according to first and last entry of webpage search
for bio in range(1, 2434):
    compurl = '/biological-activities-chemicals-csv-export/'+str(bio)+'/all?page&_format=csv'
    # Only download if file has not already been downloaded yet
    if os.path.isfile("bioact/"+str(bio)+".csv"):
        print(str(bio)+" already downloaded")
    else:
        # Send a GET request to the webpage
        res = req.get(base+compurl)
        csv = open("bioact/"+str(bio)+".csv","wb")
        csv.write(res.content)
        csv.close()
        print("Downloading "+str(bio))

# Create a folder for plant
try:
    os.mkdir("plant")
except FileExistsError:
    pass

# Change range according to first and last entry of webpage search
for bio in range(4666, 6805):
    compurl = '/plant-chemicals-csv-export/'+str(bio)+'?page&_format=csv'
    if os.path.isfile("plant/"+str(bio)+".csv"):
        print(str(bio)+" already downloaded")
    else:
        # Send a GET request to the webpage
        res = req.get(base+compurl)
        csv = open("plant/"+str(bio)+".csv","wb")
        csv.write(res.content)
        csv.close()
        print("Downloading "+str(bio))
