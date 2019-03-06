import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import csv
from collections import namedtuple

class transform_data():

    fields = ("close", "high", "low", "open", "time", "volumefrom", "volumeto", "pair")
    priceStreamRecord = namedtuple('record', fields)

    @staticmethod
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


    @staticmethod
    def read_price_data(path):
        transformer = transform_data()
        with open(path, 'rU') as data:
            data.readline()
            reader = csv.reader(data, delimiter = ",")
            list_of_rows=[]
            for row in reader:
                yield transformer.parse_price_data(row)

    @staticmethod
    def parse_schema(path="price_schema.avsc"):
        with open(path, 'r') as schema:
            return avro.schema.Parse(schema.read())

    @staticmethod
    def serialize_records(records,coin,avro_output=None):
        if avro_output==None:
            avro_output=str(coin) + ".avro"
        transformer = transform_data()
        schema = transformer.parse_schema()
        #avro_output=str(coin) + ".avro"
        with open(avro_output, 'wb') as out:
            writer = DataFileWriter(out, DatumWriter(), schema)
            for record in records:
                writer.append(record)


