import streamlit as st 
import seaborn as sns
import plotly.express as px
from shroomdk import ShroomDK
import pandas as pd
import matplotlib.pyplot as plt


st.set_page_config(page_title="Data Explorer", layout="wide",initial_sidebar_state="expanded")
st.title('Reach statitics')


# Initialize `ShroomDK` with your API Key
sdk = ShroomDK("00dba474-bd21-4d4d-a9b9-c5eaa08aac33")


ref_finance_active_addresses= f"""
SELECT block_timestamp::date as date
        , count (distinct tx_signer) AS active_addresses
FROM near.core.fact_transactions 
WHERE 1=1
  and tx_receiver = ('v2.ref-finance.near')
        AND date > CURRENT_DATE - interval ' 1 month'
        -- AND chain_name = 'avalanche_mainnet'
GROUP BY date
ORDER BY date desc

"""
ref_finance_active_addresses = sdk.query(ref_finance_active_addresses)

# Iterate over the results
# print(pd.DataFrame(query_result_set['records']))
ref_finance_active_addresses=(pd.DataFrame(ref_finance_active_addresses.records))
st.write(ref_finance_active_addresses.shape)
st.write(ref_finance_active_addresses)

ref_finance_active_addresses_fig=px.line(ref_finance_active_addresses,x='date',y='active_addresses',title='ref_finance_active_addresses_fig')

with st.container():
    col1, col2 = st.columns(2)
    with col1:
        
        st.write(ref_finance_active_addresses_fig)
    with col2:
        st.write(ref_finance_active_addresses_fig)
        


