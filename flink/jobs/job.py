from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy

env = StreamExecutionEnvironment.get_execution_environment()

source = KafkaSource.builder() \
    .set_bootstrap_servers("kafka:9092") \
    .set_topics("csv-topic") \
    .set_group_id("flink-group") \
    .set_value_only_deserializer(SimpleStringSchema()) \
    .build()

stream = env.from_source(
    source,
    WatermarkStrategy.no_watermarks(),
    "Kafka Source"
)

stream.print()

env.execute("CSV ETL")