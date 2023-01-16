# Udacity_DataEngineer_Project5
Udacity Data Engineer Nanodegree Project 5


## Requirements
You should already have a Redshift cluster created that contains a database named 'dev'. Make sure that this cluster is publically accessible, allows
access on port 5439, and has VPC Routing enabled for faster access. 

Then, you need to create an IAM user in AWS and record the user's Access Key ID and Secret Access Key. 

The redshift connection information needs to be added as a Postgres Hook named 'redshift' in Airflow, whereas the IAM user's access key and secret access key need to be added as an Amazon Web Services hook named 'aws_credentials' in Airflow. 
 
Both of these hooks need to be added to Airflow first in order for the DAG to execute since the DAG uses these hooks in its execution. 

The instructions for adding these hooks are similar to the ones provided in the 'Automate Data Pipelines' course within the various sub-courses. 


# Additional Points

* For the song_data, I only have loaded the files with the key ""song_data/A/B/A"" inside the "udacity-dend" bucket for simplicity and faster performance. 
Loading all of the song_data files takes a long time, and for proof of concept loading a small subset of files is enough. However, in case the user wishes
to load all of the song_data, they would simply need to change the 's3_key' value on line 63 of udac_example_dag.py from "song_data/A/B/A" to "song_data". 

* I moved the create_tables.sql file into a new folder named 'sql' inside the 'dags' folder so that multiple SQL create table statements can execute 
within the Postgres Operator on lines 36-40 of udac_example_dag.py. 

* It will take at least 6-8 minutes for the entire DAG to execute. 
