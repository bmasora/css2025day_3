# -*- coding: utf-8 -*-
"""
Created on Tue Jan 28 12:19:23 2025

@author: UJ
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

# McDonald is in reviews_data["App"]

subset_reviews = reviews_data[reviews_series]

aggregated_reviews = subset_reviews.groupby("App")["Sentiment_Polarity"].mean().reset_index()

joined_apps_reviews = subset_apps.merge(aggregated_reviews, on="App", how="left")

# based on this df: joined_apps_reviews filter to only show the columns
# app, rating, reviews, installs, and sentiment_polarity

filtered_apps_reviews = joined_apps_reviews[["App", "Rating", "Reviews",
                                             "Installs", 
                                             "Sentiment_Polarity"]]

print(filtered_apps_reviews.info())

"""

Data columns (total 5 columns):
 #   Column              Non-Null Count  Dtype  
---  ------              --------------  -----  
 0   App                 112 non-null    object 
 1   Rating              94 non-null     float64
 2   Reviews             112 non-null    object 
 3   Installs            112 non-null    object 
 4   Sentiment_Polarity  18 non-null     float64
dtypes: float64(2), object(3)
"""

# using filtered_apps_reviews filter based min_rating = 4.0
# min_reviews = 1000

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