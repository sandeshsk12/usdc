import streamlit as st 
import seaborn as sns
import plotly.express as px
from shroomdk import ShroomDK
import pandas as pd
from datetime import datetime as dt
import matplotlib.pyplot as plt
import time
from plotly.subplots import make_subplots


st.set_page_config(page_title="Data Explorer", layout="wide",initial_sidebar_state="expanded")
st.title('Comparing USDC across chains')


# Initialize `ShroomDK` with your API Key
sdk = ShroomDK("00dba474-bd21-4d4d-a9b9-c5eaa08aac33")
with st.spinner(text='In progress'):
    time.sleep(2)
    #st.success('Done')

with st.sidebar:
    st.markdown('# Enter variables')
    try:
        dts = st.date_input(label='Date Range: ',
                    value=(dt(year=2022, month=10, day=1, hour=0, minute=0), 
                            dt(year=2022, month=10, day=7, hour=0, minute=0)),
                    key='#date_range',
                    help="The start and end date time")
        st.write('Start: ', dts[0], "End: ", dts[1])
    except: 
        pass

    granularity=st.selectbox('Choose granularity',['hour','day','week','month','year'],index=1)
    #granularity="hour"
    st.write(granularity)

    start_date=dts[0]
    end_date=dts[1]


algorand_usd_metrics= f"""
with algo_price as 
  (
  select date_trunc({granularity},block_hour) as time, avg(price_usd) as price
  from algorand.core.ez_price_swap
  where asset_name = 'Wrapped ALGO'
  group by time
  
  ),
algo_usdc as 
  (
  select 
  	block_id, 
  	date_trunc('{granularity}',block_timestamp) as time,
  	tx_id as tx_hash, 
  	asset_sender as sender,
  	receiver,
  	asset_name as currency,
  	amount as usd_amount,
    case 
        when usd_amount < 1 and usd_amount > 0 then 'shrimp (0-1)'
        when usd_amount < 10 and usd_amount >= 1 then 'crab (1-10)'
        when usd_amount < 50 and usd_amount >= 10 then 'Octpus (10-50)'
        when usd_amount < 100 and usd_amount >= 50 then 'Fish (50-100)'
        when usd_amount < 500 and usd_amount >= 100 then 'Dolphins (100-500)'
        when usd_amount < 1000 and usd_amount >= 500 then ' Shark (500-1000)'
        when usd_amount < 5000 and usd_amount >= 1000 then ' Whale (1000-5000)'
        when usd_amount >= 5000 then 'Humpback whale (5000+)'
        else 'holder' 
    end as tx_type
  from algorand.core.ez_transfer
  where 1=1
  	and asset_id='31566704'
  	and time between '{start_date}' and '{end_date}'
  ),
daily as 
  (
select
  date_trunc('{granularity}',time) as time,
  count(distinct tx_hash) as number_of_transfers,
  sum(number_of_transfers) over (order by time asc) as cum_number_of_transfers,
  sum(usd_amount) as daily_amount,
  sum(daily_amount) over(order by time asc) as cum_daily_amount,
  count(distinct sender) as number_of_senders,
  sum(number_of_senders) over(order by time asc) as cum_senders,
  daily_amount/number_of_transfers as avg_amount_per_transafer,
  cum_daily_amount/cum_number_of_transfers as cum_avg_amount_per_transafer,
  daily_amount/number_of_senders as avg_amount_per_sender,
  cum_daily_amount/cum_senders as cum_avg_amount_per_sender
from algo_usdc 
group by time 
order by time desc 
  )
select d.*,p.price as algo_price  from daily d left join algo_price p
on d.time=p.time 
order by d.time desc  

"""
algorand_usd_metrics = sdk.query(algorand_usd_metrics)

# Iterate over the results
# print(pd.DataFrame(query_result_set['records']))
algorand_usd_metrics=(pd.DataFrame(algorand_usd_metrics.records))
fig = go.Figure(go.Indicator(
    mode = "number",
    value = 400,
    number = {'prefix': "$"},
    delta = {'position': "top", 'reference': 320},
    domain = {'x': [0, 1], 'y': [0, 1]}))
st.write(fig)



