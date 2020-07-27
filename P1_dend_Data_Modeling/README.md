# Data Modeling with Apache Cassandra

The objective is to model user activity data for a music streaming app called Sparkify. Creating a noSQL database and ETL pipeline designed to optimize queries for understanding what songs users are listening to, then model data in Apache Cassandra to allow for specific queries provided by the analytics team at Sparkify.

The dataset "event_data" contais the folowing fields:
- artist (string)
- auth (string)
- firstName (string)
- gender (char)
- itemInSession (int)
- lastName (string)
- length (float)
- level (string)
- location (string)
- method (string)
- page (string)
- registration (float)
- sessionId (int)
- song (string)
- status (int)
- ts (float)
- userId (int)

# Queries

We design the schema based on the queries we want to perform. In this case, we have three queries:

1. Find artist, song title and song length that was heard during sessionId=338, and itemInSession=4.
2. Find name of artist, song (sorted by itemInSession) and user (first and last name) for userid=10, sessionId=182.
3. Find every user name (first and last) who listened to the song 'All Hands Against His Own'.


# Schema

## Query 1:        
### Table: artist_song_session 
1. sessionId (int)
2. itemInSession (int) 
3. artist (text) 
4. song_name (text) 
5. song_length (float) 

PRIMARY KEY = (sessionId, itemInSession)

## Query 2 
### Table: artist_song_user
1. userId (int)
2. sessionId (int)
3. itemInSession (int)
4. first_name (text)
5. last_name (text)
6. artist (text) 
7. song_name (text)

PRIMARY KEY = (userId, sessionId) with clustering column: itemInSession

## Query 3
### Table: user_song
1. song_name (text)
2. userId (int)
3. first_name (text)
4. last_name (text)

PRIMARY KEY = (song_name, userId)

# Execute Instructions
Run each cell of the "Project_1B_Project_Template.ipynb".