import json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.functions import MapFunction, ReduceFunction


class ParseJSON(MapFunction):
    def map(self, value):
        try:
            data = json.loads(value)
            payload = data.get('payload', {})

            result = {
                'source_file': data.get('source_file', ''),
                'customer_id': payload.get('id', ''),
                'customer_name': f"{payload.get('customer_first_name', '')} {payload.get('customer_last_name', '')}",
                'customer_email': payload.get('customer_email', ''),
                'product_name': payload.get('product_name', ''),
                'sale_date': payload.get('sale_date', ''),
                'sale_total': payload.get('sale_total_price', ''),
                'processed_time': str(data.get('timestamp', ''))
            }
            return json.dumps(result)
        except Exception as e:
            print(f"Error parsing message: {e}")
            return ""


class SumReduce(ReduceFunction):
    def reduce(self, value1, value2):
        return value1 + value2


def main():
    print("Starting Flink ETL Job...")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    source = KafkaSource.builder() \
        .set_bootstrap_servers("kafka:9092") \
        .set_topics("csv-topic") \
        .set_group_id("flink-group") \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "Kafka Source"
    )

    parsed_stream = stream.map(ParseJSON())

    filtered_stream = parsed_stream.filter(lambda x: len(x) > 0)

    filtered_stream.print()

    count_stream = filtered_stream.map(lambda x: 1).key_by(lambda x: 'count').sum(0)
    count_stream.print()

    print("Executing Flink job...")
    env.execute("CSV to Snowflake ETL")


if __name__ == "__main__":
    main()