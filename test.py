from price_pipeline import price_pipeline
from transform_data import transform_data
import boto3

test_extract = price_pipeline()
file_name = test_extract.extract_historical_data('BTC',1546357305)
#file_name='ZRX.csv'

test_transform = transform_data()
test_transform.serialize_records(test_transform.read_price_data(file_name),'BTC')