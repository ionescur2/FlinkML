from pyflink.datastream import StreamExecutionEnvironment

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
    "bootsrap.server":"ie1-xes01-nxt.nxt.betfair:9092,ie1-xes02-nxt.nxt.betfair:9092,ie1-xes03-nxt.nxt.betfair:9092",
    "group_id": "data-dev",
    "topic": "ie1.bf.fraudDetectionTM"
}


env = StreamExecutionEnvironment.get_execution_environment()