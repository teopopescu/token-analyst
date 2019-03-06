import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import csv
from collections import namedtuple

file_path = "zrx_test_2.csv"
fields = ("close", "high", "low", "open", "time", "volumefrom", "volumeto", "pair")
priceStreamRecord = namedtuple('record', fields)


def read_forecast_data(path):
    with open(path, 'rU') as data:
        data.readline()
        reader = csv.reader(data, delimiter = ",")
        list_of_rows=[]
        for row in reader:
            row[4] = int(row[4])
            row[7] = str(row[7])
            for i in [0,1,2,3,5,6]:
                row[i]=float(row[i])
            #list_of_rows.append(row)
    #return list_of_rows
        for row in map(priceStreamRecord._make, reader): #creates a namedtuple object of time priceStreamRecord for each row in the csv
            yield row


def parse_schema(path="price_schema.avsc"):
    with open(path, 'r') as schema:
        return avro.schema.Parse(schema.read())

def serialize_records(records, avro_output="zrx_test.avro"):
    schema = parse_schema()
    with open(avro_output, 'wb') as out:
        writer = DataFileWriter(out, DatumWriter(), schema)
        for record in records:
            record = dict((f, getattr(record, f)) for f in record._fields)
            print(record)
            writer.append(record)

if __name__ == "__main__":
    serialize_records(read_forecast_data(file_path))
