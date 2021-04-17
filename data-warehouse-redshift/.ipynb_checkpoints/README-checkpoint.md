### Project: Data Warehouse

Sparkify, a music streaming startup, needs to move their processes and data onto the cloud, due to a grow in their user base and song database.

The objective of this project, is to build an ETL pipeline. The ETL pipeline should extract their data from S3, stage it in Redshift, and perform some transformations to create dimensional tables. In this way, the analytical team can continue finding insights in what songs their users are listening to.

#### Data Base Schema:
There is one main table wich stores events associated with songs plays, four dimension tables that store information related to users, songs, artists and time. There are two staging tables wich data is populated from the S3 json files.
The reason behind this schema, is that it gives flexibility to perform complex queries and answer business questions.

- Fact Table:
    1. songplays: records in event data associated with song plays

- Dimension Tables:

    1. users: users in app
    2. songs: songs in music database
    3. artists: artists in music database
    4. time: timestamps of records in songplays broken down into specific units

- Staging Tables:
    1. staging_events
    2. staging_songs


#### ETL Pipeline:
1. Create staging, fact and dimensin tables.
2. Populate the staging tables from the S3 json files
3. Populate analytics tables (dimension and staging tables) trough some transformations from staging tables.

#### How To Run the Project

1. Launch a redshift and create an IAM role with read access to S3.
2. Fill the dwh.cfg configuration file with the information from redshift cluster.
3. Run create_tables.py, to first drop and then create tables that will be used in next steps.
```
python create_tables.py
```
3. Run etl scripts elt.py to populates staging, dimensional and fact tables in redshift.
```
python etl.py
```

#### Project Structure

Files on project:
- dwh.cfg: Store information from redshift database and IAM role.
- create_tables.py: Drop and the create tables used in the etl pipeline.
- sql_queries.py: Define the queries to be executed in etl and create_tables scripts
- etl.py: Load data from S3 files to staging tables and from staging tables to analytical tables on redshift.

