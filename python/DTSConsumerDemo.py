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
        # Kafka Consumer 配置参数
        topic_name = "cn_hangzhou_vpc_rm_bp1907x8zbo20z60u_dts_upgrade_from_old_version2"
        auto_commit = False
        # 消费组 ID
        group_id = "dtse9gh4883283o991"
        sasl_mechanism = "PLAIN"
        security_protocol = "SASL_PLAINTEXT"
        username = "xiaqiutest"
        password = "DTStest1234"
        bootstrap_servers = ["dts-cn-hangzhou.aliyuncs.com:18001"]

        # 如果username不含有group_id，则更新username为username-group_id
        if group_id not in username:
            username = username + "-" + group_id

        # 创建 KafkaConsumer 实例
        consumerGroupHandler = KafkaConsumer(
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
        for msg in consumerGroupHandler:
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
