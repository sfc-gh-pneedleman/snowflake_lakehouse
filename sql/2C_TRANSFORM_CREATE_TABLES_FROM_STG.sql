--First we create a view to parse the PDF text into columns 
CREATE OR REPLACE VIEW VW_PDF_RAW_TEXT AS 
(SELECT RELATIVE_PATH, PDF_TEXT, 
LPAD(SPLIT_PART(SPLIT_PART(PDF_TEXT, 'Customer:', 2), ' ', 2), 10, 0) as CUSTOMER_ID,
SPLIT_PART(SPLIT_PART(PDF_TEXT, 'Invoice:', 2), ' ', 2) as INVOICE_NUM,
SPLIT_PART(SPLIT_PART(PDF_TEXT, 'Generated On:', 2), ' ', 2) as INV_GEN_DT,
TRIM(REPLACE(SPLIT_PART(SPLIT_PART(PDF_TEXT, 'Status:', 2), 'Payment', 1), '\n', ' ')) as INV_STATUS,
SPLIT_PART(SPLIT_PART(PDF_TEXT, 'Payment Date:', 2), ' ', 2) as PAYMENT_DT,
TO_NUMBER(SPLIT_PART(SPLIT_PART(PDF_TEXT, 'Item 1', 2), ' ', 2), '$999,999.99', 38, 2) as ITEM_1,
TO_NUMBER(SPLIT_PART(SPLIT_PART(PDF_TEXT, 'Item 2', 2), ' ', 2), '$999,999.99', 38, 2) as ITEM_2,
TO_NUMBER(SPLIT_PART(SPLIT_PART(PDF_TEXT, 'Item 3', 2), ' ', 2), '$999,999.99', 38, 2) as ITEM_3,
SPLIT_PART(SPLIT_PART(PDF_TEXT, 'Total', 2), ' ', 2)::number(38,2) as TOTAL
FROM PDF_RAW_TEXT);

--we are going to use unique IDs for INVOICE and INV_LINE PKs. Invoice IDs are only unique across days
CREATE SEQUENCE INV_ID;
CREATE SEQUENCE INV_LN_ID;

--next we create the invoice and invoice lines tables
CREATE TABLE INVOICES AS
SELECT
INV_ID.NEXTVAL as INVOICE_ID,
CUSTOMER_ID,
INVOICE_NUM as INVOICE_NUMBER,
INV_STATUS as INVOICE_STATUS,
TO_TIMESTAMP(INV_GEN_DT,'YYYY-MM-DDTHH:MI:SSZ') as INVOICE_DATE,
TRY_TO_DATE(PAYMENT_DT, 'MM/DD/YYYY') as INVOICE_PAID_DATE,
timediff('day',INVOICE_PAID_DATE ,INVOICE_DATE ) PAYMENT_TO_INVOICE_IN_DAYS,
RELATIVE_PATH as SRC_FILE_NAME
FROM VW_PDF_RAW_TEXT;

--we are using the unpivot command to take the three invoice price columns and transpose them into rows, so they can easily be summed
CREATE TABLE INVOICE_LINES AS (
SELECT INV_LN_ID.NEXTVAL as INVOICE_LINE_ID, i.INVOICE_ID, vw.INVOICE as ITEM_CD , vw.ITEMS ITEM_PRICE
FROM VW_PDF_RAW_TEXT vw
unpivot(items for invoice in (item_1, item_2, item_3)),  INVOICES I
WHERE vw.INVOICE_NUM = i.invoice_number);

--create file format to read from Parquet 
create or replace file format parquet_file_format
  type = parquet;


-- We are using schema evolution to automatically generate the table def and then load the data without specifying the columns. 
-- Snowflake can infer the schema based on the data and the match column names on the copy into operation  

--customer data is coming from our CRM's Parquet so we use the infer_schema command to generate the table. 
-- It has a unique ID that we can use 
CREATE TABLE CUSTOMER USING TEMPLATE
(select array_agg(object_construct(*))
  from table(
    infer_schema(
      location=>'@PARQUET_DATA_STAGE/customer_data/ '
      , file_format=>'parquet_file_format'
      )
    ));
--we can now load the data based on the matched column names 
COPY INTO CUSTOMER
 from @PARQUET_DATA_STAGE/customer_data
 file_format = 'parquet_file_format'
 match_by_column_name = CASE_INSENSITIVE;
 

