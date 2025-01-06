import pandas as pd
import os
import streamlit as st
from datetime import datetime, timedelta
import requests
from dotenv import load_dotenv
import time

# [ ] merge - check row by row if already present
# [ ] store main reports in parquet
# [ ] set dtypes

load_dotenv(override=True)

TICKER_BLACKLIST = [
    'VNTRF', # due to stock split
    'BRK.A', # not available in digrin
]

def map_ticker(ticker):
    if pd.isna(ticker):
        return None
        
    ticker = str(ticker).strip()
    
    ticker_map = {
        'VWCE': 'VWCE.DE',
        'VUAA': 'VUAA.DE',
        'SXRV': 'SXRV.DE',
        'ZPRV': 'ZPRV.DE',
        'ZPRX': 'ZPRX.DE',
        'MC': 'MC.PA',
        'ASML': 'ASML.AS',
        'CSPX': 'CSPX.L',
        'EISU': 'EISU.L',
        'IITU': 'IITU.L',
        'IUHC': 'IUHC.L',
        'NDIA': 'NDIA.L',
    }
    
    return ticker_map.get(ticker, ticker)

def transform(csv_filename):

    # Read input CSV
    df = pd.read_csv(os.path.join('from_t212', csv_filename))

    # Filter out blacklisted tickers
    df = df[~df['Ticker'].isin(TICKER_BLACKLIST)]
    df = df[df['Action'].isin(['Market buy', 'Market sell'])]
    
    # Apply the mapping to the ticker column
    df['Ticker'] = df['Ticker'].apply(map_ticker)
    
    # Create output filename and save
    df.to_csv(os.path.join('to_digrin', csv_filename), index=False)

@st.cache_data
def fetch_reports():

    url = "https://live.trading212.com/api/v0/history/exports"

    headers = {"Authorization": os.getenv('T212_API_KEY')}

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        return pd.DataFrame(response.json())
    else:
        st.markdown(f':x: {response.status_code=}')
        return None

def refresh_page():
    st.cache_data.clear()
    st.rerun()

def create_export(start_dt, end_dt):

    url = "https://live.trading212.com/api/v0/history/exports"

    payload = {
        "dataIncluded": {
            "includeDividends": True,
            "includeInterest": True,
            "includeOrders": True,
            "includeTransactions": True
        },
        "timeFrom": start_dt,
        "timeTo": end_dt
    }

    headers = {
        "Content-Type": "application/json",
        "Authorization": os.getenv('T212_API_KEY')
    }

    response = requests.post(url, json=payload, headers=headers)
    data = response.json()
    report_id = data['reportId']

    with st.spinner(f'Creating new export {report_id=} ...'):
        time.sleep(3)
        refresh_page()

def parse_t212_timestamp(dt_str):
    return datetime.strptime(dt_str, '%Y-%m-%dT%H:%M:%S.%fZ')

def merge_csvs(from_csv_filename, to_csv_filename):
    from_csv = pd.read_csv(os.path.join('from_t212', from_csv_filename))
    with open(os.path.join('from_t212', to_csv_filename), 'a') as to_csv:
        to_csv.write('\n')
        from_csv.to_csv(to_csv, header=False, index=False)
    refresh_page()

def main():

    st.set_page_config(layout="wide")
    st.title('T212 to Digrin Convertor')

    from_t212_csvs = [filename for filename in os.listdir('from_t212') if filename.endswith('csv')]
    digrin_csvs = [filename for filename in os.listdir('to_digrin') if filename.endswith('csv')]

    last_dts = []
    for csv_filename in from_t212_csvs:
        df = pd.read_csv(os.path.join('from_t212', csv_filename))
        last_dts.append(pd.to_datetime(df['Time'].str.split().str[0], format='%Y-%m-%d').max())

    last_dt = max(last_dts)

    st.header('New Export')

    col1, col2 = st.columns([0.8, 0.2], vertical_alignment='bottom')

    start_date, end_date = col1.date_input('Period', (last_dt+timedelta(days=1), datetime.now()))
    start_dt = datetime.combine(start_date, datetime.min.time()).strftime('%Y-%m-%dT%H:%M:%SZ')
    end_dt = datetime.combine(end_date, datetime.max.time()).strftime('%Y-%m-%dT%H:%M:%SZ')

    if col2.button('Export', use_container_width=True):
        create_export(start_dt, end_dt)

    st.header('Reports')

    if st.button('Refresh'):
        refresh_page()

    reports_df = fetch_reports()
    reports_df['reportId']=reports_df['reportId'].astype(str)
    reports_df = pd.concat([reports_df, reports_df['dataIncluded'].apply(pd.Series)], axis=1)
    reports_df = reports_df.drop(columns='dataIncluded')

    if not reports_df.empty:
        st.dataframe(reports_df)
        
        if not reports_df.query('status == "Finished"').empty:
            for report_row in reports_df.itertuples():
                col1, col2, col3 = st.columns([0.6, 0.2, 0.2], vertical_alignment='bottom')
                time_from = parse_t212_timestamp(report_row.timeFrom).strftime('%Y-%m-%d')
                time_to = parse_t212_timestamp(report_row.timeTo).strftime('%Y-%m-%d')
                csv_filename = col1.text_input('', f'{report_row.reportId}_{time_from}_{time_to}.csv')
                if col2.button(f'Download {report_row.reportId}', use_container_width=True):

                    response = requests.get(report_row.downloadLink)

                    if response.status_code == 200:
                        with open(os.path.join('from_t212', csv_filename), 'wb') as new_csv:
                            new_csv.write(response.content)
                        col3.markdown('Downloaded to "from_t212" :white_check_mark:')
                        transform(csv_filename)
                        col3.markdown('Transformed to "to_digrin" :white_check_mark:')
                    else:
                        col3.markdown(f':x: {response.status_code=}')

    else:
        pass

    st.header('CSVs library')
    csv = st.selectbox('Csv:', from_t212_csvs, index=2)
    df = pd.read_csv(os.path.join('from_t212', csv))
    st.dataframe(df.sort_values('Time', ascending=False))

    st.header('Transform')
    col1, col2, col3 = st.columns([0.7, 0.2, 0.1], vertical_alignment='bottom')
    csv_filename = col1.selectbox('Csv:', from_t212_csvs)
    if col2.button('Transform', use_container_width=True):
        transform(csv_filename)
        st.rerun()
    if csv_filename in digrin_csvs:
        col3.markdown('Transformed: :white_check_mark:')
    else:
        col3.markdown('Transformed: :x:')


    st.header('Merge')
    col1, col2, col3, col4 = st.columns([0.35, 0.35, 0.2, 0.1], vertical_alignment='bottom')
    from_csv_filename = col1.selectbox('From:', from_t212_csvs, key='from_csv')
    to_csv_filename = col2.selectbox('Append To:', from_t212_csvs, key='to_csv')
    if col3.button('Merge', use_container_width=True):
        merge_csvs(from_csv_filename, to_csv_filename)
        col4.markdown('Merged: :white_check_mark:')

main()