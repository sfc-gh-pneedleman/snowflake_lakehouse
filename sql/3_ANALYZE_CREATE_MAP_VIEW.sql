   --create view to join covid and sales data by state for the last 30 days, long and lag
create or replace view VW_MAP_SALES_COVID_DATA
 ( SUM_BY_ST, CASE_SUM,  STATE,  LAT,  LON ) as
 --get starschema lat, longs and covid data by state. only taking most last reported info in USA. 
 --using the rank to pinpoint the city with the highest cases so plot on the map for the state
 (WITH COVID_DATA AS
      (SELECT PROVINCE_STATE, LAT, LONG  , CASES, 
                SUM(CASES) OVER (PARTITION BY PROVINCE_STATE) CASE_SUM , 
                RANK() OVER (PARTITION BY PROVINCE_STATE ORDER BY CASES DESC) as RNK 
        FROM COVID19_BY_STARSCHEMA_DM.PUBLIC.JHU_COVID_19 
       WHERE LAST_REPORTED_FLAG = TRUE
         AND COUNTRY_REGION = 'United States'
         AND CASE_TYPE = 'Confirmed'
     QUALIFY RNK = 1),
  CUST_DATA as
   --get invoice sales by state for the last 30 days
    (SELECT ADDRESS_STATE, SUM(ITEM_PRICE) as ITEM_PRICE FROM CUSTOMER C, INVOICES I, INVOICE_LINES IL 
      WHERE C.CUSTOMER_ID= I.CUSTOMER_ID
        AND I.INVOICE_ID = IL.INVOICE_ID 
        AND INVOICE_DATE > CURRENT_DATE()-30 
   GROUP BY ADDRESS_STATE)
 --now we comibine Shared COVID data to Invoice data on State
 SELECT SUM(ITEM_PRICE)::float/ 1000  SUM_BY_ST, CASE_SUM::FLOAT/100 as CASE_SUM, ADDRESS_STATE as STATE, LAT, LONG AS LON FROM COVID_DATA, CUST_DATA
  WHERE CUST_DATA.ADDRESS_STATE = COVID_DATA.PROVINCE_STATE 
   GROUP BY STATE, LAT, LONG, CASE_SUM);