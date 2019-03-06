import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import csv
from collections import namedtuple

file_path = "zrx_test_2.csv"
fields = ("close", "high", "low", "open", "time", "volumefrom", "volumeto", "pair")
priceStreamRecord = namedtuple('record', fields)

def parse_price_data(row):
    return {
        "close": float(row[0]), 
        "high": float(row[1]), 
        "low": float(row[2]), 
        "open": float(row[3]), 
        "time": int(row[4]), 
        "volumefrom": float(row[5]), 
        "volumeto": float(row[6]), 
        "pair": str(row[7])
    }

def read_forecast_data(path):
    with open(path, 'rU') as data:
        data.readline()
        reader = csv.reader(data, delimiter = ",")
        list_of_rows=[]
        for row in reader:
            yield parse_price_data(row)


def parse_schema(path="price_schema.avsc"):
    with open(path, 'r') as schema:
        return avro.schema.Parse(schema.read())

def serialize_records(records, avro_output="zrx_test.avro"):
    schema = parse_schema()
    with open(avro_output, 'wb') as out:
        writer = DataFileWriter(out, DatumWriter(), schema)
        for record in records:
            writer.append(record)

if __name__ == "__main__":
    serialize_records(read_forecast_data(file_path))