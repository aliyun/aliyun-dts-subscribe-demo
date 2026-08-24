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
- ***oldest*** = *false*
  - whether to consume from the oldest offset
- ***config.Net.SASL.User*** = *"User"*
  - should be *\<username>-\<groupId>*, groupId will be set according to field ***group*** if there only exists field ***username***
- ***config.Net.SASL.Password*** = *"password"*
## Metadata and retry tuning
- ***config.Metadata.Full*** = *false*
  - a subscription channel only carries one topic, so there is no point fetching metadata for everything
- ***config.Metadata.Retry.Max*** = *5*, ***config.Metadata.Retry.Backoff*** = *1s*
  - keep this budget small: sarama rebuilds the connection and redoes the **full SASL handshake on
    every retry**, so a large budget (e.g. 150 attempts at 250ms) turns a transient server-side EOF
    into 200+ authentication attempts per minute against the DTS proxy

## Known issue: the broker advertised in metadata may be unreachable
A Kafka client first talks to the bootstrap address, then reads the **broker address advertised in the
metadata response** and connects there directly. For a public subscription that is a different IP from
the endpoint you dialled, and it is not configurable — the server hands it out.

On a machine running a per-process network filter (e.g. a macOS EDR network extension), the Go process
can be denied that address while everything else on the box is fine. Observed symptom: `connect(2)` to
the advertised IP returns `EBADF` (`connect: bad file descriptor`) on **every port**, while `nc`,
`curl`, `python3` and a JVM reach the same IP normally. The bootstrap endpoint itself gets the same
error intermittently, which is what the `Metadata.Retry` budget above absorbs.

Ruled out by testing, so not worth retrying:
- another Kafka library (`segmentio/kafka-go`, `confluent-kafka-go`) — the filter matches the process,
  not the library; even a bare `syscall.Connect` is denied
- building to a stable path instead of `go run`, and re-running `codesign`
- pinning every broker address back to the bootstrap endpoint — the endpoint answers metadata requests
  with `EOF` almost every time, it only hands out the real broker
- `config.Version` (`V0_10_0_0` … `V2_0_0_0`), `config.Metadata.Full`, `config.Net.MaxOpenRequests`

### Workaround: rewrite the broker address at dial time
Forward the blocked address through a process that *is* allowed, then point the demo at the forwarder:

```sh
python3 tools/tcpforward.py 127.0.0.1:19001 <advertised-ip>:18001
DTS_BROKER_REWRITE=<advertised-ip>:18001=127.0.0.1:19001 go run .
```

***DTS_BROKER_REWRITE*** takes `host:port=host:port` pairs, comma separated. Leave it unset in a normal
environment — the demo then dials whatever metadata advertises. Prefer allowlisting the binary in the
network filter, or running inside the VPC, over this workaround.

## Specify offset for consumption
***specifyOffset*** = *true* enables this path. Note that the standalone partition consumer
reads the same partition the consumer group is already consuming, so every record is
handled twice — set it to *false* if you only want the consumer group.

* **client** is created by `sarama.NewClient()`
* **consumer** is created by `sarama.NewConsumerFromClient(client)`
* **consumePartition** is created by `consumer.ConsumePartition(topics, partitionArea, offset)`, where offset is the specified offset, from which the partition for consumer is created
  * offset defaults to `config.Consumer.Offsets.Initial`, i.e. it follows the ***oldest*** field
  * note that the partition should not be specified, set it as default ***0***