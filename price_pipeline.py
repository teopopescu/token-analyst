import pandas as pd
import numpy as np
import requests
import json
import logging
import time


class price_pipeline():

    #function used for setting the max timestamp for each coin
    '''
    def timestamp_max_switcher(x):
        return {
        'BTC': 1546300800,  #1280016000, #15-07-2010
        'ETH': 1438300800, #31-07-2011
        'LTC': 1318550400, #14-10-2011
         'BCH': 1501632000,#2-08-2017
        'ETC': 1438300800, #31-07-2011
        'ZEC': 1477699200, #29-10-2016
         'MKR': 1441065600, #01-09-2015
        'DAI': 1464739200, #01-06-2016
        'REP': 1445990400, #28-10-2015
        'BAT': 1496448000, #03-06-2017
        'BNB': 1501027200, #26-07-2017
        'ZRX': 1502928000 #17-08-2017
    }.get(x,'No such coin')
    '''
    @staticmethod
    def timestamp_max_switcher(x):
        return {
            'BTC': 1546304461,  # 1280016000, #15-07-2010
            'ETH': 1546304461,  # 31-07-2011
            'LTC': 1546304461,  # 14-10-2011
            'BCH': 1546304461,  # 2-08-2017
            'ETC': 1546304461,  # 31-07-2011
            'ZEC': 1546304461,  # 29-10-2016
            'MKR': 1546304461,  # 01-09-2015
            'DAI': 1546304461,  # 01-06-2016
            'REP': 1546304461,  # 28-10-2015
            'BAT': 1546304461,  # 03-06-2017
            'BNB': 1546304461,  # 26-07-2017
            'ZRX': 1546304461  # 17-08-2017
        }.get(x, 'No such coin')
    logging.basicConfig(filename='data_extraction.log',level=logging.DEBUG)
    
    #function to extract historical data for coin
    def extract_historical_data(self,coin,requested_timestamp):
        api_key="1bd67a824981c5111dac94c7beca755ac6f3015ba9b60ccaa610dd700561b142"


        url= "https://min-api.cryptocompare.com/data/histohour?fsym="+ coin + "&tsym=USD&limit=10" + "&api_key="+api_key

        #Setting up 2 DataFrames, one for storing all requested data, one for storing current iteration data (2000 hours)
        price_data = pd.DataFrame()
        new_price_data=pd.DataFrame()
        r = requests.get(url)
        response = r.json()
        price_data = price_data.append(response['Data'])
        price_data['pair']=coin+'/USD'
        #max_timestamp = timestamp_max_switcher(coin)
        #max_timestamp represents the earliest point we want to get data for from the current timestamp on a historical basis
        max_timestamp = requested_timestamp

        #This conditional expression sets the timestamp used within the API request as the earliest timestamp we stored. From this point onwards, 2,000 hours are retrieved  historically.
        if not price_data[price_data['pair']==coin+'/USD'].empty:
            earliest_timestamp = price_data[price_data['pair']==str(coin)+'/USD'].sort_values(by='time')['time'].reset_index(drop=True)[0]
            url= "https://min-api.cryptocompare.com/data/histohour?fsym="+ coin + "&tsym=USD&limit=2000&toTs=" + str(earliest_timestamp)  + "&api_key="+api_key
        else:
            url= "https://min-api.cryptocompare.com/data/histohour?fsym="+ coin + "&tsym=USD&limit=2000" + "&api_key="+api_key
        while earliest_timestamp > max_timestamp:
            url= "https://min-api.cryptocompare.com/data/histohour?fsym="+ coin + "&tsym=USD&limit=2000&toTs=" + str(earliest_timestamp)  + "&api_key="+api_key
            r = requests.get(url)
            response = r.json()
            new_price_data = new_price_data.append(response['Data'])
            new_price_data['pair']=coin+'/USD'
            price_data = pd.concat([price_data,new_price_data])
            new_price_data = pd.DataFrame()
            price_data = price_data.reset_index(drop=True)
            price_data = price_data.sort_values(by='time',ascending='True')
            earliest_timestamp = price_data[price_data['pair'] == str(coin) + '/USD'].sort_values(by='time')['time'].reset_index(drop=True)[0]
        price_data.to_csv(coin + '.csv',index=False )
        return coin + '.csv'





