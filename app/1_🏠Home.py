import streamlit as st
import string

st.set_page_config(
    page_title="Data Application Home",
    page_icon="🏠",
)

st.title("Snowflake Data Applications ❄️")

st.sidebar.success("Select an application above.")

st.markdown(
    """
    #### Three applications demonstrate the power of Snowflake's unstrucuted data, data sharing, and machine larning capabilities. 
    #### Select an Application
    - ##### 📄 Customer Invoice Lookup - Browse Customer Invoice PDF files served from Snowflake.
    - ##### 🌍 COVID Sales Map - An overlay of COVID19 shared data with sales data
    - ##### ⚛️ Predictive Sales Forecast - Used a predictive model using Python's `statsmodels` library executed in Snowpark.
"""
)
st.write(" ")
st.write("Git repo: https://github.com/sfc-gh-pneedleman/snowflake_lakehouse")

#########################################################
###### SET SNOWFLAKE CONN PARAMS ONCE  ##################
#Its not good practice to include passwords in your code. 
# This is here for demo purposes
string.sf_user = "<username>"
string.sf_password='<password>' 
string.sf_account="<account>"
string.sf_role = "<role>"
string.sf_warehouse="<warehouse>"
string.sf_database="<database"
string.sf_schema="<schema>"

#########################################################
