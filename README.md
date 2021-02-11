# data-service
# landing club data analysis and data pipeline


Data Pipeline -

I've created the pipeline to injest the landing club csv via s3 to snowflake data warehouse.
I would prefer to do ELT and not ETL to put more pressure on data ingestion pipeline.
snowflake build to handle much more load and It has power and capability to transform and load the data to stage or
integration schema.

In this prototype, I have used Apache Airflow to take the file, upload to s3 with some loggings and then take the file
and copy into snowflake as it. Due to time contraint I was not able to full fill the further steps but I would clean,
transform the data into snowflake only.

Apache Airflow can be as dynamic as we want. In this prototype, I had file already available but in actual production
when we get this kind data through API Endpoints, SFTP Servers or other methods we would easily handle and build dynamic
and scalable pipeline using Airflow and Kubernetes.

S3 is our data lake and It's good to have the file there while loading into snowflake. Data Scientics can easily pickup
the raw data If they would like to build models directly off of raw data.

Enhancement wise, I would create Python Packages to make more dynamic creation of airflow dags.
For Example, If we get the csv file from 2 different sources, I would just build one common code to pull data from
both systems and create logical structure to handle both.