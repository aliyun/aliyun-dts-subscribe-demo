package main

// SIGUSR1 toggle the pause/resume consumption
import (
	"bytes"
	"context"
	"io"
	"log"
	"os"
	"os/signal"
	"sarama_demo_dts/avro"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

// Sarama configuration options
var (
	r        io.Reader
	brokers  = "dts-xxxxx.aliyuncs.com:18001"
	group    = "dtsxxxxxx"
	topics   = "cn_hangzhou_xxxxxxx_version2"
	assignor = "range"
	oldest   = true
	verbose  = false
	config   = sarama.NewConfig()
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
	var partitionArea int32 = 0
	var offset int64 = sarama.OffsetNewest
	partition, err := consumer.ConsumePartition(topics, partitionArea, offset)
	if err != nil {
		log.Panicf("Error creating partition %v according to offset %v: %v", partition, offset, err)
	}
	go func() {
		for msg := range partition.Messages() {
			log.Printf("Message on topic:%s partition:%d offset:%d\n", msg.Topic, msg.Partition, msg.Offset)
		}
	}()

	consumptionIsPaused := false
	wg := &sync.WaitGroup{}
	wg.Add(1)

	consumerGroup, err := sarama.NewConsumerGroupFromClient(group, client)
	if err != nil {
		return
	}
	go func() {
		for {
			err := consumerGroup.Consume(ctx, strings.Split(topics, ","), &consumerGroupHandler)
			if err != nil {
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
	wg.Wait()
	if err = client.Close(); err != nil {
		log.Panicf("Error closing client: %v", err)
	}
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
	for {
		select {
		case message := <-claim.Messages():
			t := avro.NewRecord()
			t, err := avro.DeserializeRecordFromSchema(bytes.NewReader(message.Value), t.Schema())
			if err != nil {
				log.Fatal(err)
			}
			// log.Println("Operation:", t.Operation)
			if t.Operation.String() != "INSERT" && t.Operation.String() != "UPDATE" && t.Operation.String() != "DELETE" {
				continue
			}

			log.Printf("Message on topic:%s partition:%d offset:%d\n", message.Topic, message.Partition, message.Offset)
			_j, _ := t.MarshalJSON()
			log.Printf("message: %s", string(_j))

			session.MarkMessage(message, "")

		case <-session.Context().Done():
			return nil
		}
	}
}
