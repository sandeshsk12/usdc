import streamlit as st 
import seaborn as sns
import plotly.express as px


st.set_page_config(page_title="Data Explorer", layout="wide",initial_sidebar_state="expanded")

st.markdown (
  """
  # Comparing near projects
  ## Introduction

  introd to near

  ## Objective 
  jansda 

  ## methodology 

  what's ip
  
  ## Glossary
  near : ioaj


  ## Key takeaways

  1. ajdba
  2. aodifjas 

  
  """
)
with st.expander('sql_code',expanded=False): 
  st.write(
    """
    gg
    """
  )
dt=st.date_input('your birthday')
a=st.number_input('enter number')
st.write(a+1025)
st.write(dt)
st.expander('sql_code',expanded=False)

st.sidebar.success("select a page above")

try:
    dts = st.date_input(label='Date Range: ',
                value=(dt(year=2022, month=5, day=20, hour=16, minute=30), 
                        dt(year=2022, month=5, day=30, hour=16, minute=30)),
                key='#date_range',
                help="The start and end date time")
    st.write('Start: ', dts[0], "End: ", dts[1])
except: 
    pass

granularity=st.selectbox('Choose granularity',['hour','day','week','month','year'])
#granularity="hour"
st.write(granularity)

from datetime import datetime as dt
try:
    dts = st.date_input(label='Date Range: ',
                value=(dt(year=2022, month=5, day=20, hour=16, minute=30), 
                        dt(year=2022, month=5, day=30, hour=16, minute=30)),
                key='#date_range',
                help="The start and end date time")
    st.write('Start: ', dts[0], "End: ", dts[1])

except:
    pass

