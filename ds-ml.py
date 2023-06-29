import json

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import RuntimeContext, MapFunction
from pyflink.common.serialization import SerializationSchema, DeserializationSchema
from pyflink.common.typeinfo import Types
from pyflink.common import WatermarkStrategy, Encoder, Types

# "topic": "ie1.bf.fraudDetectionTM"
# "$data_kafka_consumer":"data-dev" - group_id
# "broker list" : [ie1-xes01-nxt.nxt.betfair:9092,ie1-xes02-nxt.nxt.betfair:9092,ie1-xes03-nxt.nxt.betfair:9092]

"""
{
	"__extends": "../common.json",
	"consumer": {
		"type": "Kafka",
		"connection": {
			"broker_list": "$kafka_broker_ie1bf",
			"socket_max_fails": "0",
			"offset_reset": "beginning",
			"offset_auto_commit": "false",
			"offset_auto_commit_interval_ms": 600000,
			"topic_metadata_refresh_interval_ms": 600000
		}
	}
}


"""

"""
-----------
Create Environment
-----------
"""

"""
{
	"__extends": "../common.json",
	"consumer": {
		"type": "Kafka",
		"connection": {
			"broker_list": "ie1-xes201-nxt.nxt.betfair:9092,ie1-xes202-nxt.nxt.betfair:9092,ie1-xes203-nxt.nxt.betfair:9092",
			"socket_max_fails": "0",
			"offset_reset": "beginning",
			"offset_auto_commit": "false",
			"offset_auto_commit_interval_ms": 600000,
			"topic_metadata_refresh_interval_ms": 600000
		}
	}
}

# topic = ie1.bf.payments.streaming.encryptedTransactionEntity
# "group_id": "data_transaction_entity_group_id"
"channel": "input_stream"
"""


class MyMapFunction(MapFunction):
    # def open(self):
    #     pass

	def __init__(self):
		pass
    
	
	def map(self, value):
		return json.loads(value)

# class DecodeProtoSchema(SerializationSchema, DeserializationSchema):
    # def __init__(self, charset: str = 'UTF-8'):
    #     gate_way = get_gateway()
    #     j_char_set = gate_way.jvm.java.nio.charset.Charset.forName(charset)
    #     j_simple_string_serialization_schema = gate_way.jvm.com.betfair.base.utils.BetMessageSchema(j_char_set)
    #     SerializationSchema.__init__(self, j_serialization_schema=j_simple_string_serialization_schema)
    #     DeserializationSchema.__init__(self, j_deserialization_schema=j_simple_string_serialization_schema)

kafka_stream_args = {
    "bootstrap.servers":"ie1-xes301-nxt.nxt.betfair:9092,ie1-xes302-nxt.nxt.betfair:9092,ie1-xes303-nxt.nxt.betfair:9092",
    "group_id": "data-dev"
}


env = StreamExecutionEnvironment.get_execution_environment()
env.add_jars("file:///C://github//FlinkML//flink-sql-connector-kafka-1.17.1.jar")

deserialization_schema = SimpleStringSchema()
kafka_consumer = FlinkKafkaConsumer(
    topics='ie1.bf.iss.streaming.login',
    deserialization_schema=deserialization_schema,
    properties=kafka_stream_args)

ds = env.add_source(kafka_consumer)
ds = ds.map(MyMapFunction())
ds.print()
env.execute('payments_ds_1')