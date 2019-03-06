from price_pipeline import price_pipeline
from transform_data import transform_data
import boto3


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
        file_name = test_extract.extract_historical_data(coin, timestamp)
        test_transform = transform_data()
        test_transform.serialize_records(test_transform.read_price_data(file_name), coin)
        #s3 = boto3.resource('s3')
        #bucket_name = 'test-erc20-ta'
        #s3.meta.client.upload_file(coin+'.avro', bucket_name, coin.lower() + '/' + coin.lower()+ '.avro')


if __name__ == "__main__":
    timestamp_definition = price_pipeline()
    list_of_coins = ['BTC','ETH','LTC','BCH','ETC','ZEC','MKR','DAI','REP','BAT','BNB','ZRX']
    for coin in list_of_coins:
        extract_transform_load.etl(coin,1546304461)