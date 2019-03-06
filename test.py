from price_pipeline import price_pipeline
#from transform_data import transform_data

test = price_pipeline()
#test.extract_historical_data('ZRX',1502928000)

test.transform_data('ZRX')

#test_transform = transform_data()
#test_transform.serialize_records(test_transform.read_price_data('ZRX.csv'))