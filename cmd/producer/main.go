package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"time"

	kafka "github.com/ONSdigital/dp-kafka/v2"
	"github.com/ONSdigital/dp-search-data-importer/config"
	"github.com/ONSdigital/dp-search-data-importer/event"
	"github.com/ONSdigital/dp-search-data-importer/schema"
	"github.com/ONSdigital/log.go/v2/log"
)

const serviceName = "dp-search-data-importer"

func main() {
	log.Namespace = serviceName
	ctx := context.Background()

	// Get Config
	cfg, err := config.Get()
	if err != nil {
		log.Fatal(ctx, "error getting config", err)
		os.Exit(1)
	}

	// Create Kafka Producer
	pChannels := kafka.CreateProducerChannels()
	pConfig := &kafka.ProducerConfig{
		KafkaVersion: &cfg.KafkaVersion,
	}

	// KafkaTLSProtocolFlag informs service to use TLS protocol for kafka
	if cfg.KafkaSecProtocol == config.KafkaTLSProtocolFlag {
		pConfig.SecurityConfig = kafka.GetSecurityConfig(
			cfg.KafkaSecCACerts,
			cfg.KafkaSecClientCert,
			cfg.KafkaSecClientKey,
			cfg.KafkaSecSkipVerify,
		)
	}

	kafkaProducer, err := kafka.NewProducer(ctx, cfg.KafkaAddr, cfg.PublishedContentTopic, pChannels, pConfig)
	if err != nil {
		log.Fatal(ctx, "fatal error trying to create kafka producer", err, log.Data{"topic": cfg.PublishedContentTopic})
		os.Exit(1)
	}

	// kafka error logging go-routines
	kafkaProducer.Channels().LogErrors(ctx, "kafka producer")

	time.Sleep(500 * time.Millisecond)
	scanner := bufio.NewScanner(os.Stdin)
	for {
		e := scanEvent(scanner)
		log.Info(ctx, "sending hello-called event", log.Data{"helloCalledEvent": e})

		bytes, err := schema.HelloCalledEvent.Marshal(e)
		if err != nil {
			log.Fatal(ctx, "hello-called event error", err)
			os.Exit(1)
		}

		// Send bytes to Output channel, after calling Initialise just in case it is not initialised.
		kafkaProducer.Initialise(ctx)
		kafkaProducer.Channels().Output <- bytes
	}
}

// scanEvent creates a HelloCalled event according to the user input
func scanEvent(scanner *bufio.Scanner) *event.HelloCalled {
	fmt.Println("--- [Send Kafka HelloCalled] ---")

	fmt.Println("Please type the recipient name")
	fmt.Printf("$ ")
	scanner.Scan()
	name := scanner.Text()

	return &event.HelloCalled{
		RecipientName: name,
	}
}
