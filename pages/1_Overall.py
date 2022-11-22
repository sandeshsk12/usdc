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
    time.sleep(20)
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

st.markdown(" Volume ")

amount_transferred_overall= f"""
 with algo_usdc as 
  (
  select 
   'algorand' as chain,
  	block_id, 
  	date_trunc('hour',block_timestamp) as time,
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
    and usd_amount >0 
  ),
 axelar_usdc as 
  (
  select
   'axelar' as chain,
  	block_id, 
  	date_trunc('hour',block_timestamp) as time,
  	tx_id as tx_hash, 
  	sender as sender,
  	receiver as receiver,
  	CURRENCY as currency,
  	(amount/pow(10,decimal))  as usd_amount,
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
  from axelar.core.fact_transfers 
  
  where 1=1
  	and currency='uusdc'
  	and time between '{start_date}' and '{end_date}'
  ),
 sending_address as 
  (
	select *,event_data:"from" as sender,event_data:"amount" as amount from flow.core.fact_events
  		where 1=1 
  		-- and tx_id='b97a0df257483d6564917db5c8dab53c235cbf105e9e3a960bf5511f9deefe89'
		and event_contract='A.b19436aae4d94622.FiatToken'
  		and event_type='TokensWithdrawn'
  ),
receiver_address as 
  (
	select *,event_data:"to" as receiver from flow.core.fact_events
  		where 1=1 
  		-- and tx_id='b97a0df257483d6564917db5c8dab53c235cbf105e9e3a960bf5511f9deefe89'
		and event_contract='A.b19436aae4d94622.FiatToken'
  		and event_type='TokensDeposited'
  ),
flow_table as 
  (
  select sa.*,ra.receiver from receiver_address ra inner join sending_address sa on ra.tx_id=sa.tx_id
  ),
flow_usdc as 
  (
  select 
  	'flow' as chain,
    block_height as block_id, 
  	date_trunc('hour',block_timestamp) as time,
  	tx_id as tx_hash, 
  	sender as sender,
  	receiver as receiver,
  	'USDC' as currency,
  	(amount)  as usd_amount,
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
  from flow_table
  
  where 1=1
  	-- and currency='uusdc'
  	and time between '{start_date}' and '{end_date}'
  and sender!='null'
  and receiver!='null'
  and usd_amount>0
  ),
 optimism_usdc as 
  (
  select 
  	'optimism' as chain,
    block_number as block_id, 
  	date_trunc('hour',block_timestamp) as time,
  	tx_hash as tx_hash, 
  	origin_from_address as sender,
  	origin_to_address as receiver,
  	'USDC' as currency,
  	raw_amount/(pow(10,6))  as usd_amount,
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
  from optimism.core.fact_token_transfers
  
  where 1=1
    and contract_address=lower('0x7F5c764cBc14f9669B88837ca1490cCa17c31607')
  	and time between '{start_date}' and '{end_date}'
  ),
 osmosis_usdc as 
  (
  select 
  	'osmosis' as chain,
    block_id as block_id, 
  	date_trunc('hour',block_timestamp) as time,
  	tx_id as tx_hash, 
  	SENDER as sender,
  	RECEIVER as receiver,
  	'USDC' as currency,
  	amount/pow(10,6)  as usd_amount,
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
  from osmosis.core.fact_transfers
  
  where 1=1
    and currency='ibc/D189335C6E4A68B513C10AB227BF1C1D38C746766278BA3EEB4FB14124F1D858'
  	and time between '{start_date}' and '{end_date}'
  ),
 matic_usdc as 
  (
  select 
  	'polygon' as chain,
    block_number as block_id, 
  	date_trunc('hour',block_timestamp) as time,
  	tx_hash as tx_hash, 
  	origin_from_address as sender,
  	origin_to_address as receiver,
  	'USDC' as currency,
  	raw_amount/pow(10,6)  as usd_amount,
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
  from polygon.core.fact_token_transfers
  
  where 1=1
    and contract_address=lower('0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174')
  	and time between '{start_date}' and '{end_date}'
  ),
 eth_usdc as 
  (
  select 
   'ethereum' as chain,
  	block_number as block_id, 
  	date_trunc('hour',block_timestamp) as time,
  	tx_hash as tx_hash, 
  	origin_from_address as sender,
  	origin_to_address as receiver,
  	'USDC' as currency,
  	raw_amount/pow(10,6)  as usd_amount,
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
  from ethereum.core.fact_token_transfers
  
  where 1=1
    and contract_address=lower('0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48')
  	and time between '{start_date}' and '{end_date}'
  ),
 sol_usdc as 
  (
  select 
  	'Solana' as chain,
    block_id as block_id, 
  	date_trunc('hour',block_timestamp) as time,
  	tx_id as tx_hash, 
  	tx_from as sender,
  	tx_to as receiver,
  	'USDC' as currency,
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
  from solana.core.fact_transfers
  
  where 1=1
    and mint=('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v')
  	and time between '{start_date}' and '{end_date}'
  ),
 near_usdc as 
  (
  select 
  	'near' as chain,
    block_id as block_id, 
  	date_trunc('hour',block_timestamp) as time,
  	tx_hash as tx_hash, 
  	split(logs[0],' ')[3] as sender,
  	split(logs[0],' ')[5] as receiver,
  	'USDC' as currency,
  	(split(logs[0],' ')[1])/(pow(10,6))  as usd_amount,
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
  from near.core.fact_receipts
  
  where 1=1
    and receiver_id='a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48.factory.bridge.near'
    and split(logs[0],' ')[0]='Transfer'
  	and time between '{start_date}' and '{end_date}'
  ),
combined as 
(
  select * from algo_usdc
  union 
    select * from axelar_usdc
  union 
    select * from flow_usdc
  union 
    select * from optimism_usdc
  union 
    select * from osmosis_usdc
  union 
    select * from matic_usdc
  union 
    select * from eth_usdc
  union 
    select * from sol_usdc
  union 
      select * from near_usdc
  -- union 
)
select  time::date as "date",chain, sum(usd_amount) as amount,count(distinct sender) as wallets from combined
group by "date",chain
order by chain


"""
amount_transferred_overall = sdk.query(amount_transferred_overall)


amount_transferred_overall=(pd.DataFrame(amount_transferred_overall.records))
#st.write(amount_transferred_overall.shape)
#st.write(amount_transferred_overall)

overall_transferred_amount=px.area(amount_transferred_overall,x='date',y='amount',color='chain',line_group="chain",title='Volume of USDC transferred over time')
st.write(overall_transferred_amount)


with st.container():
    col1, col2 = st.columns(2)
    with col1:
        distribution_of_amount_over_chain_fig=px.pie(amount_transferred_overall,values='amount',names='chain', \
        title = 'Distribution of USDC volume over chain')
        distribution_of_amount_over_chain_fig.update_traces(textposition='inside',textinfo='percent+label')
        st.write(distribution_of_amount_over_chain_fig)
    with col2:
        distribution_of_wallets_over_chain_fig=px.pie(amount_transferred_overall,values='wallets',names='chain',labels='chain', \
        title = 'Distribution of wallets sending USDC')
        distribution_of_wallets_over_chain_fig.update_traces(textposition='inside',textinfo='percent+label')
        st.write(distribution_of_wallets_over_chain_fig)

distribution_of_transfers_overall=f"""
 with algo_usdc as 
  (
  select 
   'algorand' as chain,
  	block_id, 
  	date_trunc('hour',block_timestamp) as time,
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
    and usd_amount >0 
  ),
 axelar_usdc as 
  (
  select
   'axelar' as chain,
  	block_id, 
  	date_trunc('hour',block_timestamp) as time,
  	tx_id as tx_hash, 
  	sender as sender,
  	receiver as receiver,
  	CURRENCY as currency,
  	(amount/pow(10,decimal))  as usd_amount,
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
  from axelar.core.fact_transfers 
  
  where 1=1
  	and currency='uusdc'
  	and time between '{start_date}' and '{end_date}'
  ),
 sending_address as 
  (
	select *,event_data:"from" as sender,event_data:"amount" as amount from flow.core.fact_events
  		where 1=1 
  		-- and tx_id='b97a0df257483d6564917db5c8dab53c235cbf105e9e3a960bf5511f9deefe89'
		and event_contract='A.b19436aae4d94622.FiatToken'
  		and event_type='TokensWithdrawn'
  ),
receiver_address as 
  (
	select *,event_data:"to" as receiver from flow.core.fact_events
  		where 1=1 
  		-- and tx_id='b97a0df257483d6564917db5c8dab53c235cbf105e9e3a960bf5511f9deefe89'
		and event_contract='A.b19436aae4d94622.FiatToken'
  		and event_type='TokensDeposited'
  ),
flow_table as 
  (
  select sa.*,ra.receiver from receiver_address ra inner join sending_address sa on ra.tx_id=sa.tx_id
  ),
flow_usdc as 
  (
  select 
  	'flow' as chain,
    block_height as block_id, 
  	date_trunc('hour',block_timestamp) as time,
  	tx_id as tx_hash, 
  	sender as sender,
  	receiver as receiver,
  	'USDC' as currency,
  	(amount)  as usd_amount,
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
  from flow_table
  
  where 1=1
  	-- and currency='uusdc'
  	and time between '{start_date}' and '{end_date}'
  and sender!='null'
  and receiver!='null'
  and usd_amount>0
  ),
 optimism_usdc as 
  (
  select 
  	'optimism' as chain,
    block_number as block_id, 
  	date_trunc('hour',block_timestamp) as time,
  	tx_hash as tx_hash, 
  	origin_from_address as sender,
  	origin_to_address as receiver,
  	'USDC' as currency,
  	raw_amount/(pow(10,6))  as usd_amount,
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
  from optimism.core.fact_token_transfers
  
  where 1=1
    and contract_address=lower('0x7F5c764cBc14f9669B88837ca1490cCa17c31607')
  	and time between '{start_date}' and '{end_date}'
  ),
 osmosis_usdc as 
  (
  select 
  	'osmosis' as chain,
    block_id as block_id, 
  	date_trunc('hour',block_timestamp) as time,
  	tx_id as tx_hash, 
  	SENDER as sender,
  	RECEIVER as receiver,
  	'USDC' as currency,
  	amount/pow(10,6)  as usd_amount,
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
  from osmosis.core.fact_transfers
  
  where 1=1
    and currency='ibc/D189335C6E4A68B513C10AB227BF1C1D38C746766278BA3EEB4FB14124F1D858'
  	and time between '{start_date}' and '{end_date}'
  ),
 matic_usdc as 
  (
  select 
  	'polygon' as chain,
    block_number as block_id, 
  	date_trunc('hour',block_timestamp) as time,
  	tx_hash as tx_hash, 
  	origin_from_address as sender,
  	origin_to_address as receiver,
  	'USDC' as currency,
  	raw_amount/pow(10,6)  as usd_amount,
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
  from polygon.core.fact_token_transfers
  
  where 1=1
    and contract_address=lower('0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174')
  	and time between '{start_date}' and '{end_date}'
  ),
 eth_usdc as 
  (
  select 
   'ethereum' as chain,
  	block_number as block_id, 
  	date_trunc('hour',block_timestamp) as time,
  	tx_hash as tx_hash, 
  	origin_from_address as sender,
  	origin_to_address as receiver,
  	'USDC' as currency,
  	raw_amount/pow(10,6)  as usd_amount,
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
  from ethereum.core.fact_token_transfers
  
  where 1=1
    and contract_address=lower('0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48')
  	and time between '{start_date}' and '{end_date}'
  ),
 sol_usdc as 
  (
  select 
  	'Solana' as chain,
    block_id as block_id, 
  	date_trunc('hour',block_timestamp) as time,
  	tx_id as tx_hash, 
  	tx_from as sender,
  	tx_to as receiver,
  	'USDC' as currency,
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
  from solana.core.fact_transfers
  
  where 1=1
    and mint=('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v')
  	and time between '{start_date}' and '{end_date}'
  ),
 near_usdc as 
  (
  select 
  	'near' as chain,
    block_id as block_id, 
  	date_trunc('hour',block_timestamp) as time,
  	tx_hash as tx_hash, 
  	split(logs[0],' ')[3] as sender,
  	split(logs[0],' ')[5] as receiver,
  	'USDC' as currency,
  	(split(logs[0],' ')[1])/(pow(10,6))  as usd_amount,
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
  from near.core.fact_receipts
  
  where 1=1
    and receiver_id='a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48.factory.bridge.near'
    and split(logs[0],' ')[0]='Transfer'
  	and time between '{start_date}' and '{end_date}'
  ),
combined as 
(
  select * from algo_usdc
  union 
    select * from axelar_usdc
  union 
    select * from flow_usdc
  union 
    select * from optimism_usdc
  union 
    select * from osmosis_usdc
  union 
    select * from matic_usdc
  union 
    select * from eth_usdc
  union 
    select * from sol_usdc
  union 
      select * from near_usdc
  -- union 
)
select  chain, tx_type, sum(usd_amount) as amount,count(distinct sender) as wallets from combined
group by chain,tx_type

"""

distribution_of_transfers_overall = sdk.query(distribution_of_transfers_overall)
distribution_of_transfers_overall=(pd.DataFrame(distribution_of_transfers_overall.records))
distribution_of_transfers_overall_amount_fig=px.bar(distribution_of_transfers_overall,x='chain',y='amount',color='tx_type')
#st.write(distribution_of_transfers_overall_amount_fig)
distribution_of_transfers_overall_wallets_fig=px.bar(distribution_of_transfers_overall,x='chain',y='wallets',color='tx_type')
#st.write(distribution_of_transfers_overall_wallets_fig)


with st.container():
    col1, col2 = st.columns(2)
    with col1:
        st.write(distribution_of_transfers_overall_amount_fig)
    with col2:
        st.write(distribution_of_transfers_overall_wallets_fig)



distribution_of_transfers_day_date=f"""
 with algo_usdc as 
  (
  select 
   'algorand' as chain,
  	block_id, 
  	date_trunc('hour',block_timestamp) as time,
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
 axelar_usdc as 
  (
  select
   'axelar' as chain,
  	block_id, 
  	date_trunc('hour',block_timestamp) as time,
  	tx_id as tx_hash, 
  	sender as sender,
  	receiver as receiver,
  	CURRENCY as currency,
  	(amount/pow(10,decimal))  as usd_amount,
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
  from axelar.core.fact_transfers 
  
  where 1=1
  	and currency='uusdc'
  	and time between '{start_date}' and '{end_date}'
  ),
 sending_address as 
  (
	select *,event_data:"from" as sender,event_data:"amount" as amount from flow.core.fact_events
  		where 1=1 
  		-- and tx_id='b97a0df257483d6564917db5c8dab53c235cbf105e9e3a960bf5511f9deefe89'
		and event_contract='A.b19436aae4d94622.FiatToken'
  		and event_type='TokensWithdrawn'
  ),
receiver_address as 
  (
	select *,event_data:"to" as receiver from flow.core.fact_events
  		where 1=1 
  		-- and tx_id='b97a0df257483d6564917db5c8dab53c235cbf105e9e3a960bf5511f9deefe89'
		and event_contract='A.b19436aae4d94622.FiatToken'
  		and event_type='TokensDeposited'
  ),
flow_table as 
  (
  select sa.*,ra.receiver from receiver_address ra inner join sending_address sa on ra.tx_id=sa.tx_id
  ),
flow_usdc as 
  (
  select 
  	'flow' as chain,
    block_height as block_id, 
  	date_trunc('hour',block_timestamp) as time,
  	tx_id as tx_hash, 
  	sender as sender,
  	receiver as receiver,
  	'USDC' as currency,
  	(amount)  as usd_amount,
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
  from flow_table
  
  where 1=1
  	-- and currency='uusdc'
  	and time between '{start_date}' and '{end_date}'
  and sender!='null'
  and receiver!='null'
  and usd_amount>0
  ),
 optimism_usdc as 
  (
  select 
  	'optimism' as chain,
    block_number as block_id, 
  	date_trunc('hour',block_timestamp) as time,
  	tx_hash as tx_hash, 
  	origin_from_address as sender,
  	origin_to_address as receiver,
  	'USDC' as currency,
  	raw_amount/(pow(10,6))  as usd_amount,
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
  from optimism.core.fact_token_transfers
  
  where 1=1
    and contract_address=lower('0x7F5c764cBc14f9669B88837ca1490cCa17c31607')
  	and time between '{start_date}' and '{end_date}'
  ),
 osmosis_usdc as 
  (
  select 
  	'osmosis' as chain,
    block_id as block_id, 
  	date_trunc('hour',block_timestamp) as time,
  	tx_id as tx_hash, 
  	SENDER as sender,
  	RECEIVER as receiver,
  	'USDC' as currency,
  	amount/pow(10,6)  as usd_amount,
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
  from osmosis.core.fact_transfers
  
  where 1=1
    and currency='ibc/D189335C6E4A68B513C10AB227BF1C1D38C746766278BA3EEB4FB14124F1D858'
  	and time between '{start_date}' and '{end_date}'
  ),
 matic_usdc as 
  (
  select 
  	'polygon' as chain,
    block_number as block_id, 
  	date_trunc('hour',block_timestamp) as time,
  	tx_hash as tx_hash, 
  	origin_from_address as sender,
  	origin_to_address as receiver,
  	'USDC' as currency,
  	raw_amount/pow(10,6)  as usd_amount,
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
  from polygon.core.fact_token_transfers
  
  where 1=1
    and contract_address=lower('0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174')
  	and time between '{start_date}' and '{end_date}'
  ),
 eth_usdc as 
  (
  select 
   'ethereum' as chain,
  	block_number as block_id, 
  	date_trunc('hour',block_timestamp) as time,
  	tx_hash as tx_hash, 
  	origin_from_address as sender,
  	origin_to_address as receiver,
  	'USDC' as currency,
  	raw_amount/pow(10,6)  as usd_amount,
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
  from ethereum.core.fact_token_transfers
  
  where 1=1
    and contract_address=lower('0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48')
  	and time between '{start_date}' and '{end_date}'
  ),
 sol_usdc as 
  (
  select 
  	'solana' as chain,
    block_id as block_id, 
  	date_trunc('hour',block_timestamp) as time,
  	tx_id as tx_hash, 
  	tx_from as sender,
  	tx_to as receiver,
  	'USDC' as currency,
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
  from solana.core.fact_transfers
  
  where 1=1
    and mint=('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v')
  	and time between '{start_date}' and '{end_date}'
  ),
 near_usdc as 
  (
  select 
  	'near' as chain,
    block_id as block_id, 
  	date_trunc('hour',block_timestamp) as time,
  	tx_hash as tx_hash, 
  	split(logs[0],' ')[3] as sender,
  	split(logs[0],' ')[5] as receiver,
  	'USDC' as currency,
  	(split(logs[0],' ')[1])/(pow(10,6))  as usd_amount,
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
  from near.core.fact_receipts
  
  where 1=1
    and receiver_id='a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48.factory.bridge.near'
    and split(logs[0],' ')[0]='Transfer'
  	and time between '{start_date}' and '{end_date}'
  ),
combined as 
(
  select * from algo_usdc
  union 
    select * from axelar_usdc
  union 
    select * from flow_usdc
  union 
    select * from optimism_usdc
  union 
    select * from osmosis_usdc
  union 
    select * from matic_usdc
  union 
    select * from eth_usdc
  union 
    select * from sol_usdc
  union 
      select * from near_usdc
  -- union 
),
  tab as 
  (
  select * from combined
  -- limit 1
  ), days as 
  (select t.*,dd.DAY_OF_WEEK_NAME as day,date_part('hour',t.time) as hour from tab t inner join ethereum.core.dim_dates dd on t.time::date=dd.date_day)
select chain,day,hour,avg(usd_amount) as amount from days
group by chain,day,hour


"""

distribution_of_transfers_day_date = sdk.query(distribution_of_transfers_day_date)


distribution_of_transfers_day_date=(pd.DataFrame(distribution_of_transfers_day_date.records))
distribution_of_transfers_day_amount_datefig=px.scatter(distribution_of_transfers_day_date,x='hour',y='day',size='amount',size_max=80)
st.write(distribution_of_transfers_day_amount_datefig)


overall_price_compare=f"""
  with algo_usdc as 
  (
  select 
   'algorand' as chain,
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
 axelar_usdc as 
  (
  select
   'axelar' as chain,
  	block_id, 
  	date_trunc('{granularity}',block_timestamp) as time,
  	tx_id as tx_hash, 
  	sender as sender,
  	receiver as receiver,
  	CURRENCY as currency,
  	(amount/pow(10,decimal))  as usd_amount,
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
  from axelar.core.fact_transfers 
  
  where 1=1
  	and currency='uusdc'
  	and time between '{start_date}' and '{end_date}'
  ),
 sending_address as 
  (
	select *,event_data:"from" as sender,event_data:"amount" as amount from flow.core.fact_events
  		where 1=1 
  		-- and tx_id='b97a0df257483d6564917db5c8dab53c235cbf105e9e3a960bf5511f9deefe89'
		and event_contract='A.b19436aae4d94622.FiatToken'
  		and event_type='TokensWithdrawn'
  ),
receiver_address as 
  (
	select *,event_data:"to" as receiver from flow.core.fact_events
  		where 1=1 
  		-- and tx_id='b97a0df257483d6564917db5c8dab53c235cbf105e9e3a960bf5511f9deefe89'
		and event_contract='A.b19436aae4d94622.FiatToken'
  		and event_type='TokensDeposited'
  ),
flow_table as 
  (
  select sa.*,ra.receiver from receiver_address ra inner join sending_address sa on ra.tx_id=sa.tx_id
  ),
flow_usdc as 
  (
  select 
  	'flow' as chain,
    block_height as block_id, 
  	date_trunc('{granularity}',block_timestamp) as time,
  	tx_id as tx_hash, 
  	sender as sender,
  	receiver as receiver,
  	'USDC' as currency,
  	(amount)  as usd_amount,
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
  from flow_table
  
  where 1=1
  	-- and currency='uusdc'
  	and time between '{start_date}' and '{end_date}'
  and sender!='null'
  and receiver!='null'
  and usd_amount>0
  ),
 optimism_usdc as 
  (
  select 
  	'optimism' as chain,
    block_number as block_id, 
  	date_trunc('{granularity}',block_timestamp) as time,
  	tx_hash as tx_hash, 
  	origin_from_address as sender,
  	origin_to_address as receiver,
  	'USDC' as currency,
  	raw_amount/(pow(10,6))  as usd_amount,
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
  from optimism.core.fact_token_transfers
  
  where 1=1
    and contract_address=lower('0x7F5c764cBc14f9669B88837ca1490cCa17c31607')
  	and time between '{start_date}' and '{end_date}'
  ),
 osmosis_usdc as 
  (
  select 
  	'osmosis' as chain,
    block_id as block_id, 
  	date_trunc('{granularity}',block_timestamp) as time,
  	tx_id as tx_hash, 
  	SENDER as sender,
  	RECEIVER as receiver,
  	'USDC' as currency,
  	amount/pow(10,6)  as usd_amount,
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
  from osmosis.core.fact_transfers
  
  where 1=1
    and currency='ibc/D189335C6E4A68B513C10AB227BF1C1D38C746766278BA3EEB4FB14124F1D858'
  	and time between '{start_date}' and '{end_date}'
  ),
 matic_usdc as 
  (
  select 
  	'polygon' as chain,
    block_number as block_id, 
  	date_trunc('{granularity}',block_timestamp) as time,
  	tx_hash as tx_hash, 
  	origin_from_address as sender,
  	origin_to_address as receiver,
  	'USDC' as currency,
  	raw_amount/pow(10,6)  as usd_amount,
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
  from polygon.core.fact_token_transfers
  
  where 1=1
    and contract_address=lower('0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174')
  	and time between '{start_date}' and '{end_date}'
  ),
 eth_usdc as 
  (
  select 
   'ethereum' as chain,
  	block_number as block_id, 
  	date_trunc('{granularity}',block_timestamp) as time,
  	tx_hash as tx_hash, 
  	origin_from_address as sender,
  	origin_to_address as receiver,
  	'USDC' as currency,
  	raw_amount/pow(10,6)  as usd_amount,
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
  from ethereum.core.fact_token_transfers
  
  where 1=1
    and contract_address=lower('0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48')
  	and time between '{start_date}' and '{end_date}'
  ),
 sol_usdc as 
  (
  select 
  	'solana' as chain,
    block_id as block_id, 
  	date_trunc('{granularity}',block_timestamp) as time,
  	tx_id as tx_hash, 
  	tx_from as sender,
  	tx_to as receiver,
  	'USDC' as currency,
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
  from solana.core.fact_transfers
  
  where 1=1
    and mint=('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v')
  	and time between '{start_date}' and '{end_date}'
  ),
 near_usdc as 
  (
  select 
  	'near' as chain,
    block_id as block_id, 
  	date_trunc('{granularity}',block_timestamp) as time,
  	tx_hash as tx_hash, 
  	split(logs[0],' ')[3] as sender,
  	split(logs[0],' ')[5] as receiver,
  	'USDC' as currency,
  	(split(logs[0],' ')[1])/(pow(10,6))  as usd_amount,
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
  from near.core.fact_receipts
  
  where 1=1
    and receiver_id='a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48.factory.bridge.near'
    and split(logs[0],' ')[0]='Transfer'
  	and time between '{start_date}' and '{end_date}'
  ),
combined as 
(
  select * from algo_usdc
  union 
    select * from axelar_usdc
  union 
    select * from flow_usdc
  union 
    select * from optimism_usdc
  union 
    select * from osmosis_usdc
  union 
    select * from matic_usdc
  union 
    select * from eth_usdc
  union 
    select * from sol_usdc
  union 
      select * from near_usdc
  -- union 
),
daily_movement as 
   (
   select time,sum(usd_amount) as volume
   from combined 
   group by time 
   ),
usd_price as 
(
   select date_trunc({granularity},hour) as time,avg(price) as avg_price,avg_price-lag(avg_price) over(order by time asc) as volatility from ethereum.core.fact_hourly_token_prices
   where token_address=lower('0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48')
   and time between '{start_date}' and '{end_date}'
   group by time 
   -- limit 4
)
select dm.*,up.avg_price,up.volatility from daily_movement dm 
inner join usd_price up 
on dm.time=up.time 
order by dm.time asc




"""

overall_price_compare = sdk.query(overall_price_compare)


overall_price_compare=(pd.DataFrame(overall_price_compare.records))

subfig = make_subplots(specs=[[{"secondary_y": True}]])
fig=px.line(overall_price_compare,x='time',y='volume')
fig2=px.line(overall_price_compare,x='time',y='avg_price')
fig2.update_traces(yaxis="y2")
subfig.add_traces(fig.data + fig2.data)
subfig.layout.xaxis.title="Time"
subfig.layout.yaxis.title="Volume"
subfig.layout.yaxis2.title="Average price"


overall_price_compare_date_fig=subfig.for_each_trace(lambda t: t.update(line=dict(color=t.marker.color)))
st.write(overall_price_compare_date_fig)

subfig = make_subplots(specs=[[{"secondary_y": True}]])
fig=px.line(overall_price_compare,x='time',y='volume')
fig2=px.line(overall_price_compare,x='time',y='volatility')
fig2.update_traces(yaxis="y2")
subfig.add_traces(fig.data + fig2.data)
subfig.layout.xaxis.title="Time"
subfig.layout.yaxis.title="Volume"
subfig.layout.yaxis2.title="volatility"


overall_price_compare_date_fig=subfig.for_each_trace(lambda t: t.update(line=dict(color=t.marker.color)))
st.write(overall_price_compare_date_fig)

#subfig.show()




        


