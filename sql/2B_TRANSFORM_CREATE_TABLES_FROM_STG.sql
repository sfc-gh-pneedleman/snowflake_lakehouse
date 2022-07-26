--First we create a view to parse the PDF text into columns 
CREATE OR REPLACE VIEW VW_PDF_RAW_TEXT AS 
(SELECT RELATIVE_PATH, PDF_TEXT, 
LPAD(SPLIT_PART(SPLIT_PART(PDF_TEXT, 'Customer:', 2), ' ', 2), 10, 0) as CUSTOMER_ID,
SPLIT_PART(SPLIT_PART(PDF_TEXT, 'Invoice:', 2), ' ', 2) as INVOICE_NUM,
SPLIT_PART(SPLIT_PART(PDF_TEXT, 'Generated On:', 2), ' ', 2) as INV_GEN_DT,
TRIM(SPLIT_PART(SPLIT_PART(PDF_TEXT, 'Status:', 2), 'Payment', 1)) as INV_STATUS,
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

--create file format to read from JSON 
create or replace file format json_file_format
  type = json;



--customer data is coming from our CRM's JSON so we just need to parse the JSON. And it has a unique ID that we can use 
CREATE TABLE CUSTOMER AS
(select
t.$1:"Customer ID"::string as CUSTOMER_ID,
TRIM(SPLIT_PART(t.$1:"Customer Name"::string, ' ', 1)) as Customer_First_Name,
TRIM(SPLIT_PART(t.$1:"Customer Name"::string,' ', 2) || ' ' || SPLIT_PART(t.$1:"Customer Name"::string, ' ',3) || ' '|| SPLIT_PART(t.$1:"Customer Name"::string, ' ',4)) as Customer_last_Name,
t.$1:"Customer Name"::string as Customer_Name,
t.$1:"Customer Job"::string as customer_job,
t.$1:"Customer Company"::string as Customer_company,
t.$1:"Date of Birth"::date as BIRTH_Date,
t.$1:"Phone Numbers":cell::string as CELL_PHONE,
t.$1:"Phone Numbers":home::string as HOME_PHONE,
t.$1:"Phone Numbers":work::string as WORK_PHONE,
t.$1:"Street"::string as ADDRESS_STREET,
t.$1:"City"::string as ADDRESS_CITY  ,
t.$1:"State"::string as ADDRESS_STATE ,
t.$1:"Zip Code"::string as ADDRESS_ZIP,
t.$1:"Country Code"::string as ADDRESS_COUNTRY_CD
from @JSON_DATA_STAGE/customer_data/  (file_format => json_file_format) t);