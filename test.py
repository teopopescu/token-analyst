from price_pipeline import price_pipeline
from transform_data import transform_data

test_extract = price_pipeline()
test_extract.extract_historical_data('ZRX',1502928000)

test_transform = transform_data()
test_transform.serialize_records(test_transform.read_price_data('ZRX.csv'))