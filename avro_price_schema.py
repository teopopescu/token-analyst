from confluent_kafka import avro

# timestamp to be confirmed 
#close high  low open  time  volumefrom  volumeto  pair
VALUE_SCHEMA_STR = """
{
   "namespace": "io.tokenanalyst.pricestream",
   "name": "priceStream",
   "type": "record",
   "fields" : [
     {
       "name" : "close",
       "type" : "float"
     },
     {
       "name" : "high",
       "type" : "float"
     },
     {
       "name" : "low",
       "type" : "float"
     },
     {
       "name" : "open",
       "type" : "float"
     },
     {
       "name" : "time",
       "type" : "int"
     },
     {
       "name" : "volumefrom",
       "type" : "float"
     },
     {
       "name" : "volumeto",
       "type" : "float"
     },
       {
       "name" : "pair",
       "type" : "string"
     },
     {
       "name" : "timestamp",
       "type" : "timestamp"   
     }

    ]
}
"""

value_schema = avro.loads(VALUE_SCHEMA_STR)
