package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"time"

	kafka "github.com/ONSdigital/dp-kafka/v2"
	"github.com/ONSdigital/dp-search-data-importer/config"
	"github.com/ONSdigital/dp-search-data-importer/models"
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

	// KafkaTLSProtocol informs service to use TLS protocol for kafka
	if cfg.KafkaSecProtocol == config.KafkaTLSProtocol {
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
		log.Info(ctx, "sending search data import event", log.Data{"searchDataImportEvent": e})

		bytes, err := schema.SearchDataImportEvent.Marshal(e)
		if err != nil {
			log.Fatal(ctx, "search data import event error", err)
			os.Exit(1)
		}

		// Send bytes to Output channel, after calling Initialise just in case it is not initialised.
		_ = kafkaProducer.Initialise(ctx)
		kafkaProducer.Channels().Output <- bytes
	}
}

// scanEvent creates a searchDataImport event according to the user input
func scanEvent(scanner *bufio.Scanner) *models.SearchDataImportModel {
	fmt.Println("--- [Send Kafka PublishedContent] ---")

	fmt.Println("Type the Data Type")
	fmt.Printf("$ ")
	scanner.Scan()
	dataType := scanner.Text()

	fmt.Println("Type the JobID")
	fmt.Printf("$ ")
	scanner.Scan()
	jobID := scanner.Text()

	fmt.Println("Type the CDID")
	fmt.Printf("$ ")
	scanner.Scan()
	cdid := scanner.Text()

	fmt.Println("Type the DatasetID")
	fmt.Printf("$ ")
	scanner.Scan()
	datasetID := scanner.Text()

	fmt.Println("Type the Summary")
	fmt.Printf("$ ")
	scanner.Scan()
	summary := scanner.Text()

	fmt.Println("Type the title")
	fmt.Printf("$ ")
	scanner.Scan()
	title := scanner.Text()

	return &models.SearchDataImportModel{
		DataType:    dataType,
		JobID:       jobID,
		SearchIndex: "ONS",
		CDID:        cdid,
		DatasetID:   datasetID,
		Keywords:    []string{"keyword1", "keyword2"},
		Summary:     summary,
		ReleaseDate: "2017-09-07",
		Title:       title,
	}
}
