# Data Modeling

## Introduction

### Context:

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

### Data Base Schema:

There are 5 tables in the data base and the schema used was "Star Schema". One main table wich stores events performed by users and 4 dimentional tables that store information related to logs (users, artists, time, songs).
The reason behind this schema, is that it gives flexibility to perform complex queries and answer business questions.

### ETL Pipeline:

0. Create database and tables.
1. Create a connection to the database.
3. Visit and process all .json files inside data/song_data.
3. Extract imporant fields realted to artists and songs records.
4. Insert every record song_table and artist_table
5. Visit and process all .json files inside data/log_data.
6. Filtered "NextSong" in page attribute.
7. Parse all time related fields.
8. Insert every record parsed inside time_table.
9. Extract information related to users.
10. Insert every record inside user_table.
11. Extract events related to users logs.
12. Insert every record inside songplay_table (fact table).


