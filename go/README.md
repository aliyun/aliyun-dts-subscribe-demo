# go demo for aliyun dts subscribe

## Parameters
- ***brokers*** = *"dts-xxxxx.aliyuncs.com:18001"*
    - url, "endpoint:port"
- ***group*** = *"dtsxxxxxx"*
  - consumer group id, may start with "dts"
- ***topics*** = *"cn_hangzhou_xxxxxxx_version2"*
  - topic name
- ***assignor*** = *"range"*
  - consumer group rebalance strategy
- ***oldest*** = *true*
  - whether to consume from the oldest offset
- ***config.Net.SASL.User*** = *"User"*
  - should be *\<username>-\<groupId>*, groupId will be set according to field ***group*** if there only exists field ***username***
- ***config.Net.SASL.Password*** = *"password"*
## Specify offset for consumption
* **client** is created by `sarama.NewClient()`
* **consumer** is created by `sarama.NewConsumerFromClient(client)`
* **consumePartition** is created by `consumer.ConsumePartition(topics, partitionArea, offset)`, where offset is the specified offset, from which the partition for consumer is created
  * note that the partition should not be specified, set it as default ***0***  