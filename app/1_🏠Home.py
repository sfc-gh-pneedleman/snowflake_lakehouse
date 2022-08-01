import streamlit as st
import string
import snowflake.connector

st.set_page_config(
    page_title="Snowflake Data Application Home",
    page_icon="🏠",
)

st.title("Snowflake Data Applications ❄️")

st.sidebar.info("Please enter your Snowflake connection info, and then select an application above.")

st.markdown(
    """
    #### Three applications demonstrate the power of Snowflake's unstrucuted data, data sharing, and machine larning capabilities. 
    #### Enter your Snowflake conneciton info and then Select an Application
    - ##### 📄 Customer Invoice Lookup - Browse Customer Invoice PDF files served from Snowflake.
    - ##### 🌍 COVID Sales Map - An overlay of COVID19 shared data with sales data
    - ##### ⚛️ Predictive Sales Forecast - Used a predictive model using Python's `statsmodels` library executed in Snowpark.
"""
)
st.write("This app is based on the medium post https://medium.com/snowflake/the-snowflake-lakehouse-b583746d686b. Please follow the instrucitons before connecting to your data")
st.write("Git repo: https://github.com/sfc-gh-pneedleman/snowflake_lakehouse")

sf_user_in = st.sidebar.text_input("username")
sf_password_in = st.sidebar.text_input("Password", type='password')
sf_account_in = st.sidebar.text_input("Account Name", help="This can be in the form of <org-account> or <account_locator.region>. More info: https://docs.snowflake.com/en/user-guide/admin-account-identifier.html")
sf_role_in = st.sidebar.text_input("Role",  value="SYSADMIN")
sf_wh_in = st.sidebar.text_input("Warehouse",  value="COMPUTE_WH")
sf_db_in = st.sidebar.text_input("Database", value="SNOW_DB")
sf_schema_in = st.sidebar.text_input("Schema",  value="SNOW_SCHEMA")
# Every form must have a submit button.
submitted = st.sidebar.button("Test Connection")
if submitted:
    #########################################################
    ###### SET SNOWFLAKE CONN PARAMS ONCE  ##################
    #Its not good practice to include passwords in your code. 
    # This is here for demo purposes
    string.sf_user = sf_user_in
    string.sf_password= sf_password_in
    string.sf_account=sf_account_in
    string.sf_role = sf_role_in
    string.sf_warehouse=sf_wh_in
    string.sf_database=sf_db_in
    string.sf_schema=sf_schema_in

    #########################################################
    try:
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

        st.sidebar.success("Successfully Connected as: " + sf_user_in +  '\n' + '\n' +   \
            "You can proceed to the other pages. This connection information will be stored for your session.")
    except Exception as e:
        st.sidebar.error("Connection Failed. Please try again! The pages will not work unless a succsfull connection is made" + '\n' + '\n' + "error: " + str(e))
 