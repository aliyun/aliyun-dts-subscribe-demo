import io
import traceback

from fastavro import schema, schemaless_reader
from kafka import KafkaConsumer

dts_record_schema = schema.load_schema(
    "dts_record_avsc/com.alibaba.dts.formats.avro.Record.avsc"
)


def decode(msg_value):
    message_bytes = io.BytesIO(msg_value)
    data = schemaless_reader(message_bytes, dts_record_schema)
    return data


if __name__ == "__main__":
    try:
        consumer = KafkaConsumer(
            "cn_beijing_v************your_topic_name",
            enable_auto_commit=False,
            group_id="dtsh************_your_group_id",
            sasl_mechanism="PLAIN",
            security_protocol="SASL_PLAINTEXT",
            sasl_plain_username="your_username",
            sasl_plain_password="your_password",
            bootstrap_servers=["your_bootstrap_server.aliyuncs.com:port"],
        )
        print("start")
        for msg in consumer:
            record = decode(msg.value)
            # import datetime

            # sourceTimestamp = record.get("sourceTimestamp")
            # formatted_date = datetime.datetime.fromtimestamp(sourceTimestamp).strftime(
            #     "%Y-%m-%d %H:%M:%S"
            # )
            # print(formatted_date)
            print(record)

        print("end")
    except Exception as e:
        print(e)
        traceback.print_exc()
