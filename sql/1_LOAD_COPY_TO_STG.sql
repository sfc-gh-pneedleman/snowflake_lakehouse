/* Database and schema setup */ 
--Create a new Database, schema and stages
CREATE DATABASE SNOW_DB;
CREATE SCHEMA SNOW_SCHEMA;
--create stage for Parquet data
CREATE STAGE PARQUET_DATA_STAGE;
--create stage for PDF files using directory param and encryption 
CREATE STAGE PDF_FILE_STAGE
directory = (enable = true )
ENCRYPTION = (TYPE =  'SNOWFLAKE_SSE');
/* Load data files into the newly created Snowflake stages */ 
--upload customer data. Note this command should be run via SnoqSQL 

-- #### RUN the below command via SnowSQL to uplaod data to your Stage ####
--put file://parquet/customer_data/* @parquet_data_stage/customer_data/;
-- ####                                                     


--note this command to upload PDFs will take about 10min for me given that 1GB of data going through my upload speed 
-- ##### RUN the below command via SnowSQL to uplaod data to your Stage #####
--put file://pdfs/* @pdf_file_stage AUTO_COMPRESS=false;
--#######

--since this is a directory stage used to support unstructured data, we must refresh the directory structure 
ALTER STAGE PDF_FILE_STAGE REFRESH;
/* lets review our objects and take a quick look at the stages */ 
LIST @PARQUET_DATA_STAGE;
LIST @PDF_FILE_STAGE;
