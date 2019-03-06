from price_pipeline import price_pipeline
from transform_data import transform_data
import boto3
#from crontab import CronTab
import time
from datetime import datetime, timedelta

#This class contains code for extracting historical data for 14 cryptocurrency pairs on an hourly basis.

class extract_transform_load():

    #A function used to extract cryptocurrency data up to a certain timestamp.
    '''
    Parameters:
    @coin - cryptocurrency code in string format
    @timestamp- earliest unix timestamp (i.e. requesting data up to a certain point in the past)
    '''
    @staticmethod
    def etl(coin,timestamp):
        test_extract = price_pipeline()
        file_name = test_extract.extract_historical_data(coin, timestamp,historical=False)
        test_transform = transform_data()
        test_transform.serialize_records(test_transform.read_price_data(file_name), coin,historical=False)
        #s3 = boto3.resource('s3')
        #bucket_name = 'test-erc20-ta'
        #s3.meta.client.upload_file(coin+'.avro', bucket_name, coin.lower() + '/' + coin.lower()+ '.avro')

    #function to work as a job at every :01 of the hour;
    # extract latest hour cryptocurrency price data, transform it to avro and send it to the right bucket.

    '''
    @staticmethod
    def extract_ongoing_data(coin):

        last_hour= datetime.now() - timedelta(hours=1)
        unixtime = time.mktime(last_hour.timetuple())
        job = cron.new(command=extract_transform_load.etl(coin,int(unixtime)))
        job.hour.every(1)
    '''

if __name__ == "__main__":
    timestamp_definition = price_pipeline()
    list_of_coins = ['BTC','ETH','LTC','BCH','ETC','ZEC','MKR','DAI','REP','BAT','BNB','ZRX']
    for coin in list_of_coins:
        max_timestamp = timestamp_definition.timestamp_max_switcher(coin)
        #extract_transform_load.etl(coin, int(max_timestamp))
        extract_transform_load.etl(coin, 0)


