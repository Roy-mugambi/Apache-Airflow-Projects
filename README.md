## Project Defination

Both **Project 1** and **Project 2** showcase the use of Airflow's Directed Acyclic Graphs (DAGs) to author, schedule and manage workflows. 

The two apache projects involve:
- Pulling raw datasets from S3.
- Validating schema compliance.
- Computing genre-level and hourly-level KPIs.
- Storing the results into a database for reporting.
- Archiving processed raw stream files in S3.

### Project 1 (Real-time Distributed Music stream Processing with Glue, Airflow & DynamoDB)

The project focuses on processing spotify music streams that are randomly uploaded into an S3 bucket for **genre level** and **Hourly KPIs**.

#### Architecture Overview:
1. **Amazon S3 -** Data Source
     - Songs, Users and Streams datasets are stored here. Also archives the processed files.
2. **AWS Glue -** Performs heavy transformation logic using Spark and Upserts the processed KPIs into DynamoDB
3. **Airflow Dag:**
     - Scheduled to check for any new streams after every 5 mins.
     - Performs validation checks by ensuring all required files are present and they all contain the required columns
     - If validation check passed, triggers python spark job in glue to enrich data and compute KPI
     - Finally triggers glue job to upsert processed KPIs to DynamoDB for further processing and archives processed CSV file in S3.
4. **DynamoDB -** Stores the processed genre level and hourly level KPIs. 


### Project 2 (Batch Music Stream processing with Airflow & Redshift)
