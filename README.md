# Snowflake Lakehouse Demo

### These files are based on the medium article post: URL 

Here are the high-level steps to get started:
  1. Downlaod the /files to local and use SnowSQL or another connector to put the files to Snowflake Stage: `sql/1_LOAD_%`
  2. Transform and Process the data using the SQL statemenetns located in: `sql/2%_TRANSFORM_%`
  3. Run the streamlit app (`/app`) locally to showcase the power of Streamlit + Snowflake
      * Modfify the Snowflake connection params in the `1_🏠Home.py` file
      * In terminal with Python and Streamlit installed: `streamlit run 1_🏠Home.py`


The post that accompanies this post can be found here: https://medium.com/snowflake/the-snowflake-lakehouse-b583746d686b
