from jsonschema import Draft202012Validator
import streamlit as st
import pandas as pd
import pydeck as pdk
from urllib.error import URLError
import numpy as np
import snowflake.connector
import string

st.set_page_config(page_title="COVID Sales Map", page_icon="🌍", layout="wide")

##add some markdown to the page with a desc 
st.header("COVID19 Sales Map")

st.write( """
##### This applications uses StreamLit's  [`st.pydeck_chart`] to display geospatial data based on two different elements
##### 
  - **COVID19 data is brought into Snowflake via the Marketplace provided by [Starschema](https://www.snowflake.com/datasets/starschema-covid-19-epidemiological-data)**  
    The number of covid cases are represented by the red circles for each state. \
    The larger circles have more active covid cases. This data is updated daily.
  - **Monthly Sales data for the last 30 days for each state**  
    This data is represented by the bars ontop of the red circles.  The higher the bar the higher the sales for the state in the past 30 days. 
"""
)

##use a spinner while data is loading. more fun than looking a white screen :) 
with st.spinner('Generating Map'):

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

    #open the connection
    cs = ctx.cursor()

    #select the data from our pre-computed view 
    SQL_STMT ="SELECT  STATE, SUM_BY_ST, CASE_SUM, LAT, LON  FROM VW_MAP_SALES_COVID_DATA"
    cs.execute(SQL_STMT)


    #create a dataframe from the query result 
    df1 = cs.fetch_pandas_all()



    #format our numbers by creatning two new rows with commas and $s
    df1['formatted_sum'] = df1['SUM_BY_ST'].apply(lambda x: "${:.1f}k".format((x/1000)))
    df1['formatted_cases'] = df1['CASE_SUM'].apply(lambda x: "{:,.0f}".format(x))


    #use our new formatted columns in the tooltips 
    tooltip = {
        "html":
            "<b>State Name:</b> {STATE} <br/>"
            "<b>Last 30 Day Sales:</b> {formatted_sum} <br/>"
            "<b>Covid Cases:</b> {formatted_cases} <br/>",
        "style": {
            "backgroundColor": "lightgrey",
            "color": "black",
        }
    }

    ####################################################################################################
    # Generate the Map 
    # ---------------------------------------------------------------------------------------------------
    # We are using pydeck to display the data with two layers 
    # Layer 1: th column layer with the round columns coming off the chart to display our sales by state
    # Layer 2: scatterplot circles that's radius coresponds to the number of covid cases reported
    #######################################################################################################
    st.pydeck_chart(pdk.Deck(
        map_style='mapbox://styles/mapbox/light-v9',
        initial_view_state=pdk.ViewState(
            latitude=39.2626,
            longitude=-94.54,
            zoom=4,
            pitch=60,
            height=800
        ),
        layers=[
            pdk.Layer(
                'ColumnLayer',
                data=df1,
                get_position='[LON, LAT]',
                radius=30000,
                elevation_scale=300,
                get_elevation=['SUM_BY_ST'],
                get_fill_color=['SUM_BY_ST' *255 , ( 'SUM_BY_ST') * 255 , 128,  100],
                #get_fill_color=[['SUM_BY_ST'] * 255 ,  ['SUM_BY_ST'] * 255 , ['SUM_BY_ST'] *128 ,  75],
                pickable=True,
                auto_highlight=True,
            ),
            pdk.Layer(
                'ScatterplotLayer',            
                data=df1,
                pickable=True,
                opacity=0.5,
                stroked=True,
                filled=True,
                radius_scale=6,
                get_position='[LON, LAT]',
                get_color='[200, 30, 0, 160]',
                get_radius=['CASE_SUM'],
            ),
        ],
        tooltip=tooltip
    ))

