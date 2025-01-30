# -*- coding: utf-8 -*-
"""
Created on Tue Jan 28 11:06:05 2025

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


# Take a peek at one of the DataFrames


"""
We have extracted the data and now we want to transform them. 

Now we are going to use the food and drink category. 

So we are going to write a function that provides 
a top apps view for food and drink. 

So we will write a function that takes in 5 parameters, drop some duplicates
find positive reviews and filter columns. Then only keep a few columns. 

Then join it by min_rating and min_reviews, order it and check for min rating 
of 4 stars with at least 1000 reviews.

"""

print(apps_data["Category"].unique())

"""
Data has been ingested
['ART_AND_DESIGN' 'AUTO_AND_VEHICLES' 'BEAUTY' 'BOOKS_AND_REFERENCE'
 'BUSINESS' 'COMICS' 'COMMUNICATION' 'DATING' 'EDUCATION' 'ENTERTAINMENT'
 'EVENTS' 'FINANCE' 'FOOD_AND_DRINK' 'HEALTH_AND_FITNESS' 'HOUSE_AND_HOME'
 'LIBRARIES_AND_DEMO' 'LIFESTYLE' 'GAME' 'FAMILY' 'MEDICAL' 'SOCIAL'
 'SHOPPING' 'PHOTOGRAPHY' 'SPORTS' 'TRAVEL_AND_LOCAL' 'TOOLS'
 'PERSONALIZATION' 'PRODUCTIVITY' 'PARENTING' 'WEATHER' 'VIDEO_PLAYERS'
 'NEWS_AND_MAGAZINES' 'MAPS_AND_NAVIGATION' '1.9']
"""

category = "FOOD_AND_DRINK"
min_rating = 4.0
min_reviews = 1000

reviews_data = reviews_data.drop_duplicates()
apps_data = apps_data.drop_duplicates(subset=["App"])

apps_series = apps_data["Category"] == "FOOD_AND_DRINK"

print(apps_series.describe())

"""
0        False
1        False
2        False
3        False
4        False
 
10836    False
10837    False
10838    False
10839    False
10840    False
Name: Category, Length: 9660, dtype: bool

count      9660
unique        2
top       False
freq       9548
Name: Category, dtype: object
"""

subset_apps = apps_data[apps_series]

print(subset_apps.head())

"""
                                                    App  ... Android Ver
1176                                         McDonald's  ...  4.4 and up
1177                              Easy and Fast Recipes  ...  2.3 and up
1178  Cookpad - FREE recipe search makes fun cooking...  ...  5.0 and up
1179  DELISH KITCHEN - FREE recipe movies make food ...  ...  4.1 and up
1180           Sumine side dish - dish recipe side dish  ...  4.1 and up

[5 rows x 13 columns]
"""

reviews_series = reviews_data["App"].isin(subset_apps["App"])

#McDonald is in reviews_data["App"]

subset_reviews =  reviews_data[reviews_series]

aggregated_reviews = subset_reviews.groupby("App")["Sentiment_Polarity"].mean().reset_index()

joined_apps_reviews = subset_apps.merge(aggregated_reviews, on="App", how= "left")

#based on this df: joined_apps_reviews filter to only show the columns
#appp, rating, reviews, installs, and sentimental_polarity

filtered_apps_reviews = joined_apps_reviews[["App", "Rating", "Reviews", "Installs", "Sentiment_Polarity"]]

print(filtered_apps_reviews.info())

"""
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 127 entries, 0 to 126
Data columns (total 5 columns):
 #   Column              Non-Null Count  Dtype  
---  ------              --------------  -----  
 0   App                 127 non-null    object 
 1   Rating              109 non-null    float64
 2   Reviews             127 non-null    object 
 3   Installs            127 non-null    object 
 4   Sentiment_Polarity  23 non-null     float64
dtypes: float64(2), object(3)
memory usage: 5.1+ KB
None

"""




#using filtered_apps_reviews filter based min_rating = 4.0
#min_reviews = 1000


filtered_apps_reviews["Reviews"] = filtered_apps_reviews["Reviews"].astype(int)


# min_rating = 4.0
rating_series = filtered_apps_reviews["Rating"] >= min_rating
apps_with_min_rating = filtered_apps_reviews[rating_series]

# min_reviews = 1000
#top_apps = 

reviews_series = apps_with_min_rating["Reviews"] >= min_reviews
top_apps = apps_with_min_rating[reviews_series]

top_apps = top_apps.sort_values(by=["Rating","Reviews"]
                                ,ascending=False).reset_index(drop=True)

"""
A value is trying to be set on a copy of a slice from a DataFrame.
Try using .loc[row_indexer,col_indexer] = value instead

See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
  filtered_apps_reviews["Reviews"] = filtered_apps_reviews["Reviews"].astype(int)

"""

# Transform Function
def transform(apps, reviews, category, min_rating, min_reviews):
    
    
      
       # Drop any duplicates from both DataFrames
       reviews_data = reviews.drop_duplicates()
       apps_data = apps.drop_duplicates(subset=["App"])
       
       # Find all of the apps and reviews in the food and drink category
       apps_series = apps_data["Category"] == category

       subset_apps = apps_data[apps_series]

       reviews_series = reviews_data["App"].isin(subset_apps["App"])

       subset_reviews = reviews_data[reviews_series]
    
    
    
    # Aggregate the subset_reviews DataFrame
    aggregate_reviews - subset_reviews.groupby("App")
    
    
    
    # Join it back to the subset_apps table
    
    # Keep only the needed columns
    
    # Convert reviews, keep only values with an average rating of at least 4 stars, and at least 1000 reviews
    
    # Sort the top apps, replace NaN with 0, reset the index (drop, inplace)
    
    # Persist this DataFrame as top_apps.csv file
    
    # Print what has happened so far
    
    # Return the transformed DataFrame
    return top_apps
    
# Call the function
top_apps_data = transform(
    apps=apps_data, 
    reviews=reviews_data,
    category="FOOD_AND_DRINK",
    min_rating=4.0,
    min_reviews=1000)




# Transform Function

    # Print statement for observability
    
    # Drop any duplicates from both DataFrames (also have the option to do this in-place)
    
    # Find all of the apps and reviews in the food and drink category
    
    # Aggregate the subset_reviews DataFrame
    
    # Join it back to the subset_apps table
    
    # Keep only the needed columns
    
    # Convert reviews, keep only values with an average rating of at least 4 stars, and at least 1000 reviews
    
    # Sort the top apps, replace NaN with 0, reset the index (drop, inplace)
    
    # Persist this DataFrame as top_apps.csv file
    
    # Print what has happened so far
    
    # Return the transformed DataFrame
    
# Call the function

# Show the data




"""
Ok so last step is to load data, now you can save and keep it as csv
 but for it is 
a better practice to load it into sqlite DB or similar if its quite 
a large file.
...what advantages are there of loading into a SQL DB?
"""

# Import sqlite

# Load Function

    # Create a connection object
    
    # Write the data to the specified table (table_name)
    
    # Read the data, and return the result (it is to be used)
    
    # Add try/except to handle error handling and assert to check for conditions
    
# Call the function
    