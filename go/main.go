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
	"github.com/linkedin/goavro/v2"
)

// Sarama configuration options
var (
	r       io.Reader
	brokers = "ip:port"
	group    = "dtsgroup"
	topics   = "dtstopic"
	assignor = "range"
	oldest   = true
	verbose  = false
)

func main() {
	keepRunning := true
	log.Println("Starting a new Sarama consumer")

	if verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Net.MaxOpenRequests = 100
	config.Consumer.Offsets.CommitInterval = 1 * time.Second
	config.Net.SASL.Enable = true
	config.Net.SASL.User = "user"
	config.Net.SASL.Password = "password"
	config.Version = sarama.V0_11_0_0

	switch assignor {
	case "sticky":
		config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategySticky()}
	case "roundrobin":
		config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	case "range":
		config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRange()}
	default:
		log.Panicf("Unrecognized consumer group partition assignor: %s", assignor)
	}

	if oldest {
		config.Consumer.Offsets.Initial = sarama.OffsetOldest
	} else {
		config.Consumer.Offsets.Initial = sarama.OffsetNewest
	}

	/**
	 * Setup a new Sarama consumer group
	 */
	consumer := Consumer{
		ready: make(chan bool),
	}
	ctx, cancel := context.WithCancel(context.Background())

	client, err := sarama.NewConsumerGroup(strings.Split(brokers, ","), group, config)
	if err != nil {
		log.Panicf("Error creating consumer group client: %v", err)
	}

	consumptionIsPaused := false
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		for {
			if err := client.Consume(ctx, strings.Split(topics, ","), &consumer); err != nil {
				log.Panicf("Error from consumer: %v", err)
			}
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()

	<-consumer.ready // Await till the consumer has been set up
	log.Println("Sarama consumer up and running!...")

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
			toggleConsumptionFlow(client, &consumptionIsPaused)
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

type Consumer struct {
	ready chan bool
}

func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message := <-claim.Messages():
			r = bytes.NewReader(message.Value)
			_, err := avro.DeserializeRecord(r)
			if err != nil {
				log.Fatal(err)
			}
			t := avro.NewRecord()
			codec, err := goavro.NewCodec(t.Schema())
			if err != nil {
				log.Fatal(err)
			}

			native, _, err := codec.NativeFromBinary(message.Value)
			if err != nil {
				log.Fatal(err)
			}

			textual, err := codec.TextualFromNative(nil, native)
			if err != nil {
				log.Fatal(err)
			}

			//nativeMap := native.(map[string]interface{})
			//if nativeMap["operation"].(string) != "HEARTBEAT" {
			//log.Println("native:", native, "operation:", nativeMap["operation"])
			log.Println("texual:", string(textual))
			//}

			session.MarkMessage(message, "")

		case <-session.Context().Done():
			return nil
		}
	}
}
