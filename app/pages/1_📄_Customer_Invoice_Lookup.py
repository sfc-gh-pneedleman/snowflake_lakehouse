
##import python libraries  
from dataclasses import replace
import snowflake.connector
from snowflake.connector.cursor import ResultMetadata
import streamlit as st
import sys
from datetime import datetime
import base64
import urllib.request
import pandas as pd
import string

st.set_page_config(page_title="Customer Invoice Lookup", page_icon="📄") 


st.header('Customer Invoice Lookup')

st.write(
"""
##### Search for a Customer and then their resulting invoices. The PDF documents are being served direclty from the Snowflake Stage \
through a [directory table](https://docs.snowflake.com/en/user-guide/data-load-dirtables.html).
"""
)

##using caching to speed up the refresh of going back to get the list of values for each selection. There could be an issue with Presigned URLs timing out. 
#If you see 401 errors disable the cache
@st.cache(persist=False)
def get_data():

    ##snowflake connection info. Its not good practice to include passwords in your code. This is here for demo purposes
    ctx = snowflake.connector.connect(
        user=string.sf_user,
        password=string.sf_password, 
        account=string.sf_account,
        warehouse=string.sf_warehouse,
        database=string.sf_database,
        schema=string.sf_schema,
        role=string.sf_role
    )

    #open a snowflake cursor
    cs = ctx.cursor()

    ##this SQL statement is where the magic happens for retrieving files. 
    # We can easily get the customer and invoice info but we also have the unique file name that can be used to join to 
    # our directory table (stage). Once we have our stage we use the get_presigned_url() function to get the AWS URL to our file 
    sql_stmt = "SELECT c.CUSTOMER_ID, CUSTOMER_LAST_NAME || ', ' || CUSTOMER_FIRST_NAME as CUST_NAME,  INVOICE_NUMBER, I.SRC_FILE_NAME,   \
                    get_presigned_url(@PDF_FILE_STAGE, relative_path) as FILE                                                             \
                FROM CUSTOMER C, INVOICES I, DIRECTORY(@PDF_FILE_STAGE) PDF_STG                                                           \
                WHERE c.customer_id = I.CUSTOMER_ID                                                                                       \
                AND I.SRC_FILE_NAME = PDF_STG.RELATIVE_PATH (+);"                                                                                             

    #execute the SQL and store results in a pandas dataframe
    cs.execute(sql_stmt)
    df = cs.fetch_pandas_all()

    return df

#now that we have the dataframe we can gen the PDF
def get_pdf_url(df):

    # Select box options -
    # instead of stacking the select boxes we can put them side by side in 2 columns 
    col1, col2 = st.columns(2)

    #customer 
    with col1:
        #get/display the customer name 
        option = st.selectbox('Customer:', df['CUST_NAME'])
        
    #invoice
    with col2: 

        #filter the results to the invoices associated to the selected customer
        df_filtered = df.loc[df['CUST_NAME'] == option]

        #get/display invoices based on the selected customer 
        option2 = st.selectbox('Invoice', df_filtered['SRC_FILE_NAME'])


    #now that we have our selected invoice number, we can get the unique PDF name from the invoice name 
    row= df_filtered.loc[df_filtered['SRC_FILE_NAME'] == str(option2)]

    #and with the invoice name we can get the URL and store it in a variable. Remember the FILE value comes from the Snowflake SQL 
    file_url = row['FILE'].values[0]

    return file_url

#function to display the PDF of a given file 
def displayPDF(file):
    # Opening file from file path. this is used to open the file from a website rather than local
    with urllib.request.urlopen(file) as f:
        base64_pdf = base64.b64encode(f.read()).decode('utf-8')

    # Embedding PDF in HTML
    pdf_display = F'<iframe src="data:application/pdf;base64,{base64_pdf}" width="700" height="950" type="application/pdf"></iframe>'

    # Displaying File
    st.markdown(pdf_display, unsafe_allow_html=True)

 


#main 
if __name__ == "__main__":

    df = get_data()
    #get the URL from the associated stage 
    file_url = get_pdf_url(df)
    #call the display PDF function with the provided URL 
    displayPDF(file_url)