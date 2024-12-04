
####
# PDF  viewer to read from Snowflake Stage wthin Streamlit in Snowflake
# Uses SNOWFLAKE.CORTEX.PARSE_DOCUMENT to dynamically parse document 
####

##import python libraries  
import streamlit as st
import base64
from snowflake.snowpark.context import get_active_session


st.set_page_config(page_title="Customer Invoice Lookup", page_icon="📄", layout="wide") 


st.header('Customer Invoice Lookup')


st.write(
"""
##### Search for a Customer and then their resulting invoices. The PDF documents are being served direclty from the Snowflake Stage \
through a [directory table](https://docs.snowflake.com/en/user-guide/data-load-dirtables.html).
"""
)

##using caching to speed up the refresh of going back to get the list of values for each selection. There could be an issue with Presigned URLs timing out. 
#If you see 401 errors disable the cache
@st.cache_data
def get_data():
    
    session = get_active_session()

    ##this SQL statement is where the magic happens for retrieving files. 
    # We can easily get the customer and invoice info but we also have the unique file name that can be used to join to 
    # our directory table (stage). Once we have our stage we use the get_presigned_url() function to get the AWS URL to our file 
    # we also splice the first and last name and then concatinate its formatted Last, First. 
    sql_stmt = "SELECT c.CUSTOMER_ID,                                                                       \
                    TRIM(SPLIT_PART(NAME, ' ', 1)) as FIRST_NAME ,                                 \
                    TRIM(SPLIT_PART(NAME,' ', 2)      || ' '||                                     \
                        SPLIT_PART(NAME, ' ',3)       || ' '||                                     \
                        SPLIT_PART(NAME, ' ',4)) as LAST_NAME ,                                    \
                  LAST_NAME || ', ' || FIRST_NAME AS CUST_NAME,                                             \
                  INVOICE_NUMBER, I.SRC_FILE_NAME,                                                          \
                    get_presigned_url(@PDF_FILE_STAGE, relative_path) as FILE                               \
                FROM CUSTOMER C, INVOICES I, DIRECTORY(@PDF_FILE_STAGE) PDF_STG                             \
                WHERE c.customer_id = I.CUSTOMER_ID                                                         \
                AND I.SRC_FILE_NAME = PDF_STG.RELATIVE_PATH (+);"                                                                                             

    #execute the SQL and store results in a pandas dataframe
    df = session.sql(sql_stmt).to_pandas()

    return df

#now that we have the dataframe we can gen the PDF
def get_pdf_url(df):

    session = get_active_session()

    # Select box options -
    # instead of stacking the select boxes we can put them side by side in 2 columns 
    col1, col2 = st.columns(2)

    #customer 
    with col1:
        #get/display the customer name 
        option = st.selectbox('Customer:', df['CUST_NAME'])

        #filter the results to the invoices associated to the selected customer
        df_filtered = df.loc[df['CUST_NAME'] == option]

        #get/display invoices based on the selected customer 
        option2 = st.selectbox('Invoice', df_filtered['SRC_FILE_NAME'])

        pdf_file_name = option2
        #st.write(pdf_file_name)
        

    #now that we have our selected invoice number, we can get the unique PDF name from the invoice name 
    row= df_filtered.loc[df_filtered['SRC_FILE_NAME'] == str(option2)]

    #and with the invoice name we can get the URL and store it in a variable. Remember the FILE value comes from the Snowflake SQL 
    file_url = row['FILE'].values[0]
    #st.write(file_url)

    stage = "PDF_FILE_STAGE"

    session.file.get(f"@{stage}/{pdf_file_name}", f"/tmp")
    with open(f"/tmp/"+pdf_file_name, "rb") as f:
        content_bytes = f.read()

    #assign return variables 
    content_b64encoded = base64.b64encode(content_bytes).decode('utf-8')
    pdf_name = pdf_file_name


    return content_b64encoded, pdf_name


#function to display the PDF of a given file 
def displayPDF(file, file_name):
    # Opening file from file path. this is used to open the file from a website rather than local

    
    hreflink = f'<br  /><b>Google Chrome Users</b>, right click and open in new tab: <a href="data:application/pdf;base64,{file}" target="_blank" download="{file_name}">{file_name}</a>'
    st.markdown(hreflink, unsafe_allow_html=True)

    
    # Embedding PDF in HTML
    pdf_display = F'''<iframe src="data:application/pdf;base64,{file}" width="700" height="950" type="application/pdf"></iframe>'''
    st.markdown(pdf_display, unsafe_allow_html=True)
    

def pdf_text(file_name):

    session = get_active_session()

    ##dislay 
    st.write('')
    st.write('Custom PDF Extract Text Output')
    text_sql_cust = f"""SELECT * EXCLUDE (PDF_TEXT) FROM VW_PDF_RAW_TEXT
                    WHERE RELATIVE_PATH = '{file_name}'"""
    
    #execute the SQL and store results in a pandas dataframe
    df_text_cust = session.sql(text_sql_cust).to_pandas()
    st.dataframe(df_text_cust)

 
    st.write('')
    st.write('Document Parse PDF Extract Output')
    text_sql_parse = f"""
       SELECT COLUMN_NAME, COLUMN_VALUE, VALUE, CONTENT_ARRAY FROM (
        SELECT '{file_name}',
      SNOWFLAKE.CORTEX.PARSE_DOCUMENT(
        @PDF_FILE_STAGE,
        '{file_name}',
        {{'mode': 'OCR'}}
      ) AS layout, Layout:content::string as content,
      SPLIT(content, '\n') content_array, 
      a.VALUE::string as VALUE,
      a.INDEX,
      CASE WHEN a.INDEX < 9 THEN SPLIT_PART(VALUE::string,':', 1) WHEN a.INDEX BETWEEN 9 AND 11 THEN  SPLIT_PART(VALUE::string,'$', 1) ELSE SPLIT_PART(VALUE::string,' ', 1) END COLUMN_NAME,
      CASE WHEN a.INDEX < 9 THEN SPLIT_PART(VALUE::string,':', 2) WHEN a.INDEX BETWEEN 9 AND 11 THEN  SPLIT_PART(VALUE::string,'$', 2) ELSE SPLIT_PART(VALUE::string,' ', 2) END COLUMN_VALUE
      FROM TABLE(FLATTEN(input=>content_array)) a
      WHERE A.INDEX NOT BETWEEN 1 AND 4
      )"""

    df_text_parse = session.sql(text_sql_parse).to_pandas()
    st.dataframe(df_text_parse)


#main 
if __name__ == "__main__":

    df = get_data()
    #get the URL from the associated stage 
    file_url, file_name = get_pdf_url(df)
    #call the display PDF function with the provided URL 
    displayPDF(file_url, file_name)
    pdf_text(file_name)
