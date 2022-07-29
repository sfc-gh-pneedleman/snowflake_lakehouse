-- #####################################
-- ## Process the files with our UDF
-- ###################################

--this process is quite memory intensive. Lets use the power of Snowflake to instantly scale to process all the PDFs
ALTER WAREHOUSE COMPUTE_WH SET WAREHOUSE_SIZE = '2X-Large';
--save all the PDF data along with the file name into a new table in its text format 
CREATE TRANSIENT TABLE PDF_RAW_TEXT AS 
(select RELATIVE_PATH, process_pdf(file_url) as Pdf_text
from (
    select file_url,RELATIVE_PATH
    from directory(@pdf_file_stage)
    group by file_url, RELATIVE_PATH
)) ;
--scale the compute back down once complete
ALTER WAREHOUSE COMPUTE_WH SET WAREHOUSE_SIZE = 'X-Small';