# Data Lake

### Context:

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a s3 file of JSON logs on user activity on the app, as well as a s3 with JSON metadata on the songs in their app.

### Data Base Schema:

There are 5 tables in the data base and the schema used was "Star Schema". One main table wich stores events performed by users and 4 dimentional tables that store information related to logs (users, artists, time, songs).
The reason behind this schema, is that it gives flexibility to perform complex queries and answer business questions.

### ETL Pipeline:

1. Read data from input path declared in dl.cfg
2. Using spark, transform the json files into the database schema describes in last point
3. Load it back to the output path as parquet files. Each table will have a separated folder.

### How To Run the Project

1. Populate/Change the configuration in dl.cfg. The input data and output data can be an s3 path or a local path.

2. Run in console
```
python etl.py
```

### Project Structure

Files on project:

- dl.cfg: File with AWS keys and paths to configure.
- etl.py: Script that loads, transform and generates the parquet files.
- Readme.md: Documentation of the project
