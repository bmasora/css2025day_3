# -*- coding: utf-8 -*-
"""
Created on Tue Jan 28 10:49:09 2025

@author: UJ
"""

# -*- coding: utf-8 -*-
"""
Data Pipeline for ETL

In this code-along, we'll focus on extracting data from flat-files. 
A flat file might be something like a .csv or a .json file. 
The two files that we'll be extracting data from are the apps_data.csv and 
the review_data.csv file. To do this, we'll used pandas. 
Let's take a closer look!

"""

# Import Pandas
import pandas as pd

# Ingest these datasets into memory using read_csv and save as apps and reviews variable
apps = pd.read_csv("apps_data.csv")
reviews = pd.read_csv("review_data.csv")

# Take peak at the two data sets with print function or view in variable explorer

# View the columns, shape and data types of the data sets
print(apps.columns)
print(apps.shape)
print(apps.dtypes)

"""
Index(['App', 'Category', 'Rating', 'Reviews', 'Size', 'Installs', 'Type',
       'Price', 'Content Rating', 'Genres', 'Last Updated', 'Current Ver',
       'Android Ver'],
      dtype='object')
(10841, 13)
App                object
Category           object
Rating            float64
Reviews            object
Size               object
Installs           object
Type               object
Price              object
Content Rating     object
Genres             object
Last Updated       object
Current Ver        object
Android Ver        object
dtype: object
"""

# Is there a single pandas method that does this?
print(apps.info())

"""
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 10841 entries, 0 to 10840
Data columns (total 13 columns):
 #   Column          Non-Null Count  Dtype  
---  ------          --------------  -----  
 0   App             10841 non-null  object 
 1   Category        10841 non-null  object 
 2   Rating          9367 non-null   float64
 3   Reviews         10841 non-null  object 
 4   Size            10841 non-null  object 
 5   Installs        10841 non-null  object 
 6   Type            10840 non-null  object 
 7   Price           10841 non-null  object 
 8   Content Rating  10840 non-null  object 
 9   Genres          10841 non-null  object 
 10  Last Updated    10841 non-null  object 
 11  Current Ver     10833 non-null  object 
 12  Android Ver     10838 non-null  object 
dtypes: float64(1), object(12)
memory usage: 2.5+ MB
None
"""

# Can you see the data in variable explorer?

"""
The code above works perfectly well, but this time let's try using 
DRY-principles  to build a function to extract data.

Create a function called extract, with a single parameter of name file_path.
Print the number of rows and columns in the DataFrame, as well as the data 
type of each column. 
Provide instructions about how to use the value that will eventually be 
returned by this function. Return the variable data.
Call the extract function twice, once passing in the apps_data.csv file path, 
and another time with the review_data.csv file path. Output the first 
few rows of the apps_data DataFrame.

Extracting is one of the most tricky things to do in a data pipeline, 
always try to know much as you can about the source system, 
here its just a flat file which is quite simple.
"""

# Extract Function
def extract(file_path):

    # Read the file into memory
    data = pd.read_csv(file_path)
    
    # Now, print the details about the file
    print(data.info())
    
    # Print the type of each column
    
    # Finally, print a message before returning the DataFrame
    print("Data has been ingested")
    
    return data


# Call the function (create apps_data and reviews_data)
apps_data = extract("apps_data.csv")
reviews_data = extract("review_data.csv")