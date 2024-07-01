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
        # Kafka Consumer configuration parameters
        topic_name = "cn_hangzhou_vpc_rm_xxxxxxxx_dts_upgrade_from_old_version2"
        auto_commit = False
        # Consumer group ID
        group_id = "dtsexxxxxxxx"
        sasl_mechanism = "PLAIN"
        security_protocol = "SASL_PLAINTEXT"
        username = "username"
        password = "password"
        bootstrap_servers = ["dts-{region}.aliyuncs.com:18001"]

        # If the username does not contain the group_id, update the username to username-group_id
        if group_id not in username:
            username = username + "-" + group_id

        # Create a KafkaConsumer instance
        consumer = KafkaConsumer(
            topic_name,
            enable_auto_commit=auto_commit,
            group_id=group_id,
            sasl_mechanism=sasl_mechanism,
            security_protocol=security_protocol,
            sasl_plain_username=username,
            sasl_plain_password=password,
            bootstrap_servers=bootstrap_servers,
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
