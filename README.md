# Sparkify Data project
The purpose of this project is to build an ETL pipeline for a data lake hosted on S3. Currently, Sparkify data reside in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

In the project, an ETL pipeline will be designed to read/extract data from S3, process the data into analytics tables using Spark and load them back into S3 as dimensional tables. The Spark process will be deployed on a cluster using AWS.

Upon completion, this process will allow the analytics team to continue finding insights in what songs their users are listening to. 

###### Project Files
data folder - The folder contains the song_data and log_data zip files. The song_data zip file contains the song_data JSON log files 
which contain information on the songs. The log_data zip file contains the user activity log files which contain information on the user activities. 

dl.cfg - This cponfig file contains AWS credentials

etl.py - This python script is used to load the whole datasets. It contains procedures for creating database connection, processing log files and closing the connection. 

###### Steps To Read and Load Data 
Spin up a cluster
Open the cmd terminal in the Launcher
Execute the etl.py script to run the ETL process 
Validate that the tables - songplays, users, songs, artists, time and their parquet files are created. 