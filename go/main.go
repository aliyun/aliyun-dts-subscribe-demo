package main

// SIGUSR1 toggle the pause/resume consumption
import (
	"bytes"
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aliyun/aliyun-dts-subscribe-demo/go/avro"

	"github.com/IBM/sarama"
	"github.com/linkedin/goavro/v2"
)

// 可选的 broker 地址改写，格式 "host:port=host:port"，多组用逗号分隔。
// 仅在本机网络策略拦掉 metadata 返回的 broker 地址时才需要，正常环境不要设置。
const brokerRewriteEnv = "DTS_BROKER_REWRITE"

// Sarama configuration options
var (
	brokers  = "dts-xxxxx.aliyuncs.com:18001"
	group    = "dtsxxxxxx"
	topics   = "cn_hangzhou_xxxxxxx_version2"
	assignor = "range"
	oldest   = false
	verbose  = false
	// 开启后额外用独立的分区消费者从指定 offset 拉取，会和 consumer group 重复消费同一分区，
	// 仅用于演示指定 offset 的用法。
	specifyOffset = true
	config        = sarama.NewConfig()
)

func main() {
	keepRunning := true
	log.Println("Starting a new Sarama consumerGroupHandler")

	if verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	config.Consumer.Return.Errors = true
	config.Net.MaxOpenRequests = 100
	config.Consumer.Offsets.CommitInterval = 1 * time.Second
	config.Net.SASL.Enable = true
	config.Net.SASL.User = "User"
	config.Net.SASL.Password = "password"
	config.Version = sarama.V0_11_0_0
	// DTS 一个订阅通道只有一个 topic，没必要拉全量 metadata
	config.Metadata.Full = false
	// 注意：sarama 每次 metadata 重试都会重建连接并重做一遍 SASL 握手。
	// 预算给大了（比如 150 次）会在服务端偶发 EOF 时变成每分钟两百多次认证，
	// 反而像在压测 DTS 代理，把偶发失败拖成持续失败。
	config.Metadata.Retry.Max = 5
	config.Metadata.Retry.Backoff = 1 * time.Second

	// Kafka 客户端拿到 metadata 后会直连其中通告的 broker 地址，这个地址由服务端下发，
	// 无法通过配置指定。若本机网络策略拦掉了该地址，只能在拨号这一层改写。
	if mapping := parseBrokerRewrite(os.Getenv(brokerRewriteEnv)); len(mapping) > 0 {
		log.Printf("broker address rewrite enabled: %v", mapping)
		config.Net.Proxy.Enable = true
		config.Net.Proxy.Dialer = &rewriteDialer{
			mapping: mapping,
			base:    &net.Dialer{Timeout: config.Net.DialTimeout},
		}
	}

	// 如果User不含有group，则更新User为User-group
	if !strings.Contains(config.Net.SASL.User, group) {
		config.Net.SASL.User = config.Net.SASL.User + "-" + group
	}

	switch assignor {
	case "sticky":
		config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategySticky()}
	case "roundrobin":
		config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	case "range":
		config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRange()}
	default:
		log.Panicf("Unrecognized consumerGroupHandler group partition assignor: %s", assignor)
	}

	if oldest {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	} else {
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
	}

	/**
	 * Set up a new Sarama consumerGroupHandler group
	 */
	consumerGroupHandler := ConsumerGroupHandler{
		ready: make(chan bool),
	}

	ctx, cancel := context.WithCancel(context.Background())

	client, err := sarama.NewClient(strings.Split(brokers, ","), config)
	if err != nil {
		log.Panicf("Error creating client: %v", err)
	}

	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		log.Panicf("Error creating consumer: %v", err)
	}

	consumptionIsPaused := false
	wg := &sync.WaitGroup{}

	var partitionConsumer sarama.PartitionConsumer
	if specifyOffset {
		var partitionArea int32 = 0
		offset := config.Consumer.Offsets.Initial
		partitionConsumer, err = consumer.ConsumePartition(topics, partitionArea, offset)
		if err != nil {
			log.Panicf("Error creating consumer for partition %v according to offset %v: %v", partitionArea, offset, err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for msg := range partitionConsumer.Messages() {
				log.Printf("Message on topic:%s partition:%d offset:%d\n", msg.Topic, msg.Partition, msg.Offset)
			}
		}()
	}

	consumerGroup, err := sarama.NewConsumerGroupFromClient(group, client)
	if err != nil {
		log.Panicf("Error creating consumer group: %v", err)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			err := consumerGroup.Consume(ctx, strings.Split(topics, ","), &consumerGroupHandler)
			if err != nil {
				// 关闭流程里 cancel() 先执行，Consume 因此返回的错误不算异常
				if ctx.Err() != nil {
					return
				}
				log.Panicf("Error from consumerGroupHandler: %v", err)
			}
			if ctx.Err() != nil {
				return
			}
			consumerGroupHandler.ready = make(chan bool)
		}
	}()

	<-consumerGroupHandler.ready // Await till the consumerGroupHandler has been set up
	log.Println("Sarama consumerGroupHandler up and running!...")

	sigusr1 := make(chan os.Signal, 1)
	signal.Notify(sigusr1, syscall.SIGUSR1)

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	for keepRunning {
		select {
		case <-ctx.Done():
			log.Println("terminating: context cancelled")
			keepRunning = false
		case <-sigterm:
			log.Println("terminating: via signal")
			keepRunning = false
		case <-sigusr1:
			toggleConsumptionFlow(consumerGroup, &consumptionIsPaused)
		}
	}
	cancel()
	if partitionConsumer != nil {
		if err = partitionConsumer.Close(); err != nil {
			log.Printf("Error closing partition consumer: %v", err)
		}
	}
	wg.Wait()
	if err = consumerGroup.Close(); err != nil {
		log.Printf("Error closing consumer group: %v", err)
	}
	if err = consumer.Close(); err != nil {
		log.Printf("Error closing consumer: %v", err)
	}
	if err = client.Close(); err != nil {
		log.Panicf("Error closing client: %v", err)
	}
}

// rewriteDialer 在拨号前把 broker 地址替换成 mapping 指定的地址，未命中的地址原样连接。
type rewriteDialer struct {
	mapping map[string]string
	base    *net.Dialer
}

func (d *rewriteDialer) Dial(network, addr string) (net.Conn, error) {
	if to, ok := d.mapping[addr]; ok {
		log.Printf("dialing %s instead of %s", to, addr)
		addr = to
	}
	return d.base.Dial(network, addr)
}

func parseBrokerRewrite(spec string) map[string]string {
	mapping := make(map[string]string)
	for _, pair := range strings.Split(spec, ",") {
		from, to, ok := strings.Cut(strings.TrimSpace(pair), "=")
		if !ok {
			continue
		}
		mapping[strings.TrimSpace(from)] = strings.TrimSpace(to)
	}
	return mapping
}

func toggleConsumptionFlow(client sarama.ConsumerGroup, isPaused *bool) {
	if *isPaused {
		client.ResumeAll()
		log.Println("Resuming consumption")
	} else {
		client.PauseAll()
		log.Println("Pausing consumption")
	}

	*isPaused = !*isPaused
}

type ConsumerGroupHandler struct {
	ready chan bool
}

func (consumer *ConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

func (consumer *ConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (consumer *ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	codec, err := goavro.NewCodec(avro.NewRecord().Schema())
	if err != nil {
		return err
	}

	for {
		select {
		case message, ok := <-claim.Messages():
			if !ok {
				log.Println("message channel was closed")
				return nil
			}
			if _, err := avro.DeserializeRecord(bytes.NewReader(message.Value)); err != nil {
				log.Printf("Skipping message on topic:%s partition:%d offset:%d, deserialize failed: %v", message.Topic, message.Partition, message.Offset, err)
				continue
			}

			native, _, err := codec.NativeFromBinary(message.Value)
			if err != nil {
				log.Printf("Skipping message on topic:%s partition:%d offset:%d, decode failed: %v", message.Topic, message.Partition, message.Offset, err)
				continue
			}

			textual, err := codec.TextualFromNative(nil, native)
			if err != nil {
				log.Printf("Skipping message on topic:%s partition:%d offset:%d, encode to json failed: %v", message.Topic, message.Partition, message.Offset, err)
				continue
			}

			nativeMap := native.(map[string]interface{})
			if nativeMap["operation"].(string) != "HEARTBEAT" && nativeMap["operation"].(string) != "BEGIN" && nativeMap["operation"].(string) != "COMMIT" {
				log.Println("native:", native, "operation:", nativeMap["operation"])
				log.Println("texual:", string(textual))
			}

			session.MarkMessage(message, "")

		case <-session.Context().Done():
			return nil
		}
	}
}
