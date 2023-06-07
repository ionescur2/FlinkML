from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema

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
ds.print()
env.execute
('payments_ds_1')