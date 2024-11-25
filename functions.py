import datetime as dt
import re
import os
import pandas as pd
import requests
from dotenv import load_dotenv
from alphacast import Alphacast

def download_amb_informal_er():
    """ This functions downloads and formats the informal exchange rate data 
    from Ambito Financiero"""
    # Setting up the dates needed to scrape the data
    start_date = '01-01-1900'
    today_date = dt.date.today().strftime('%d-%m-%Y')

    # The URL to get the data from
    url = ('https://mercados.ambito.com//dolar/informal/historico-general' +
           f'/{start_date}/{today_date}')

    # A header to stop the server from refusing the request
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}

    # Making the request
    response = requests.get(url, headers=headers, timeout=30)

    if response.status_code == 200:
    
        # Matching and creating a list of strings
        rx = re.compile(r'\[(.*?)\]') 
        results = rx.findall(response.text[1:-1])

        # Creating a list of lists
        rx = re.compile(r'\"(.*?)\"')
        for i in enumerate(results):
            results[i[0]] = rx.findall(i[1])

        # Creating the DataFrame
        df = pd.DataFrame(results, columns=results[0])
        df = df.loc[1:, :].reset_index(drop=True)

        # Renaming the columns
        df = df.rename(columns={'Fecha': 'Date', 'Compra': 'bid_price',
                       'Venta': 'ask_price'})

        # Fixing the date column
        df['Date'] = (df['Date'].str.replace('\\/', '/')
                      .apply(lambda x: pd.to_datetime(x, format='%d/%m/%Y')))

        # Fixing the float columns.
        df['bid_price'] = (df['bid_price'].str.replace(',', '.')
                           .astype('float16'))
        df['ask_price'] = (df['ask_price'].str.replace(',', '.')
                           .astype('float16'))

        # Adding the Country column
        df['country'] = 'Argentina'

        # Changing the order of the columns
        df = df[['Date', 'country','bid_price', 'ask_price']]

        # Dropping duplicate rows
        df = df.drop_duplicates(subset='Date')

        # Sorting by date
        df = df.sort_values(by='Date').reset_index(drop=True)

        # Change the date type to dt.date
        df['Date'] = df['Date'].dt.date

        return df

def get_last_date(df):
    """ This function returns the last date from a dataframe"""
    return df.iloc[-1, 0]

def download_ac_data(ac, dataset_id):
    """ This function downloads data from alphacast"""
    df = ac.datasets.dataset(dataset_id).download_data("pandas")
    df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d').dt.date
    return df

def update_dataset(ac, df, dataset_id):
    """ This functions updates a dataset from alphacast"""
    if df is not None:
        (ac.datasets.dataset(dataset_id)
                    .upload_data_from_df(df,
                                         deleteMissingFromDB = False,
                                         onConflictUpdateDB = False,
                                         uploadIndex=False))

def format_new_cpi(new_cpi):
    """ This functions corrects the format of CPI data"""
    new_cpi = new_cpi.loc[:, ['Date', 'Nivel general']]
    new_cpi = new_cpi.rename(columns={'Nivel general': 'CPI'})
    return new_cpi

def check_and_merge_cpi_data(old_cpi, new_cpi):
    """ This functions checks if there's any new CPI data and if there is it 
    merges it with the old data"""
    # Checking the dates
    last_old_cpi_date = get_last_date(old_cpi)
    last_new_cpi_date = get_last_date(new_cpi)

    # If the old cpi doesn't have the newest data
    if last_old_cpi_date < last_new_cpi_date:
        # Remove the data covered by the new index from the old one
        cutoff_date = dt.date(2017, 1, 1)
        old_cpi = old_cpi.loc[old_cpi['Date'] < cutoff_date, :]

        # Calculate the inflation and remove the first row on the new one
        new_cpi['inflation'] = new_cpi['CPI'].pct_change()
        new_cpi = new_cpi.iloc[1:, :].copy()

        # Concat both
        cpi = pd.concat((old_cpi, new_cpi)).reset_index(drop=True)
        return cpi
    
    # If the old one is up to date
    else:
        return None
    
def create_ac_object():
    load_dotenv()
    ac = Alphacast(os.getenv('ALPHACAST_KEY'))
    return ac

def upload_data():
    # Creating the Alphacast object
    ac = create_ac_object()

    # Checking and updating the dataset for the informal exchange rate
    amb_informal_er = download_amb_informal_er()
    ac_informal_er = download_ac_data(ac, 29762)
    last_amb_er_date = get_last_date(amb_informal_er)
    last_ac_er_date = get_last_date(ac_informal_er)
    if last_ac_er_date < last_amb_er_date:
        update_dataset(ac, amb_informal_er, 29762)
        print('The informal_er data has been updated')
    else:
        print("There's no need to update the informal_er data")

    # Checking and updating the dataset for the cpi data
    new_cpi = download_ac_data(ac, 5515)
    new_cpi = format_new_cpi(new_cpi)
    old_cpi = download_ac_data(ac, 29891)
    cpi = check_and_merge_cpi_data(old_cpi, new_cpi)
    if cpi is not None:
        update_dataset(ac, cpi, 29891)
        print('The cpi data has been updated')
    else:
        print("There's no need to update the cpi data")