# SMITHSONIAN to Postgress
### Author: Dreycey Albin

## Breif Description
This repository contains an automated pipeline for scraping smithsonian
OpenAccess metadata and inserting it into postgress. The current pipeline is
limited to postgress, but the script has been configured in such a way to allow
other databases to maintain the metadata. It was built to dynamically build the
database, so if the metadata changes, so does the way the database is built.
Below are the instructions for how to run everything in the repo. Everything can
essentially be ran with one command once the postgress database has been
created.

## future features

* image scraper is in the code, not currently used. 
* ability to make import for other database types

## Build the postgresDB
The first thing you need to do before using the master bash script is build a
database. This is the most convienent way to build the database: 

Login to psql using: 
```
psql
```

within the psql command line interface:
```
CREATE DATABASE smithsoniandb2;
```

Then quit using: 
```
\q
```

## Executing the master bash script

There is a master script that downloads, builds, and inserts the data into the
database you created above. Below is a schematic of the overall master script
works. It is currently being extended to work with other databases, but for now
it is limited to postgress- though the build is completely automated. 

To execute the master script: 
```
bash MakePostgresDB.sh 
```

This command will take some time to run, especially for downloading and the
subsequent parsing that needs to take place. This will create input csv files
for each of the metadata directories, and thereafter will create and
susbequently execute a bash file running psql commands for building the
database.


