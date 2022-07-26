/* Database and schema setup */ 
--Create a new Database, schema and stages
CREATE DATABASE SNOW_DB;
CREATE SCHEMA SNOW_SCHEMA;
--create stage for JSON data
CREATE STAGE JSON_DATA_STAGE;
--create stage for PDF files using directory param and encryption 
CREATE STAGE PDF_FILE_STAGE
directory = (enable = true )
ENCRYPTION = (TYPE =  'SNOWFLAKE_SSE');

-- Load data files into the newly created Snowflake stages 

-- #####
-- ## Upload Customer data:
-- ## Run the below command via SnowSQL to uplaod  data to your Stage.
-- ## Relative path to execute this command is from ./files/
-- ######
put file://json/customer_data/* @json_data_stage/customer_data/;                                                 
/* end put */

-- ##### 
-- ## Upload PDF Invoices: 
-- ## Note this command to upload PDFs will take about 10min for me given that 1GB based on my upload speed 
-- ## Run the below command via SnowSQL to uplaod data to your Stage 
-- ## Relative path to execute this command is from ./files/
-- ##### 
put file://pdfs/* @pdf_file_stage AUTO_COMPRESS=false;
/* end put */

--since this is a directory stage used to support unstructured data, we must refresh the directory structure 
ALTER STAGE PDF_FILE_STAGE REFRESH;
