import argparse
import logging
import sys
import os
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import Configuration, WatermarkStrategy
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

args = {
    "bootsrap.server":"ie1-xes201-nxt.nxt.betfair:9092,ie1-xes202-nxt.nxt.betfair:9092,ie1-xes203-nxt.nxt.betfair:9092",
    "group_id": "data_transaction_entity_group_id",
    "topic": "ie1.bf.payments.streaming.encryptedTransactionEntity"
}

env = StreamExecutionEnvironment.get_execution_environment()
env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
env.set_parallelism(1)


kafka_jar = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                            'flink-sql-connector-kafka_2.11-1.13.0.jar')

jar_path = "file://{}".format(kafka_jar)

env_config = Configuration()
env_config.set_string("pipeline.jars", jar_path)

env.configure(env_config)

kafka_source = FlinkKafkaConsumer(topics="ie1.bf.payments.streaming.encryptedTransactionEntity",\
            deserialization_schema=SimpleStringSchema(),\
            properties=args)

datastream = env.add_source(kafka_source).print()

env.execute('Kafka')
