package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	kafka "github.com/ONSdigital/dp-kafka/v3"
	"github.com/ONSdigital/dp-search-data-importer/config"
	"github.com/ONSdigital/dp-search-data-importer/models"
	"github.com/ONSdigital/dp-search-data-importer/schema"
	"github.com/ONSdigital/log.go/v2/log"
)

const serviceName = "dp-search-data-importer"

const (
	ZebedeeDataType = "legacy"
	DatasetDataType = "datasets"
)

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
	pConfig := &kafka.ProducerConfig{
		BrokerAddrs:     cfg.Kafka.Addr,
		Topic:           cfg.Kafka.PublishedContentTopic,
		KafkaVersion:    &cfg.Kafka.Version,
		MaxMessageBytes: &cfg.Kafka.MaxBytes,
	}
	if cfg.Kafka.SecProtocol == config.KafkaTLSProtocol {
		pConfig.SecurityConfig = kafka.GetSecurityConfig(
			cfg.Kafka.SecCACerts,
			cfg.Kafka.SecClientCert,
			cfg.Kafka.SecClientKey,
			cfg.Kafka.SecSkipVerify,
		)
	}
	kafkaProducer, err := kafka.NewProducer(ctx, pConfig)
	if err != nil {
		log.Fatal(ctx, "fatal error trying to create kafka producer", err, log.Data{"topic": cfg.Kafka.PublishedContentTopic})
		os.Exit(1)
	}

	// kafka error logging go-routines
	kafkaProducer.LogErrors(ctx)

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
		err = kafkaProducer.Initialise(ctx)
		if err != nil {
			log.Fatal(ctx, "failed to initialise kafka producer", err)
			os.Exit(1)
		}
		kafkaProducer.Channels().Output <- bytes
	}
}

// scanEvent creates a searchDataImport event according to the user input
func scanEvent(scanner *bufio.Scanner) *models.SearchDataImport {
	fmt.Println("--- [Send Kafka PublishedContent] ---")

	fmt.Println("Type the UID")
	fmt.Printf("$ ")
	scanner.Scan()
	uid := scanner.Text()

	fmt.Printf("Type the Data Type (%s or %s)\n", DatasetDataType, ZebedeeDataType)
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

	searchDataImport := &models.SearchDataImport{
		UID:         uid,
		DataType:    dataType,
		JobID:       jobID,
		SearchIndex: "kamen",
		CDID:        cdid,
		DatasetID:   datasetID,
		Keywords:    []string{"keyword1", "keyword2"},
		Summary:     summary,
		ReleaseDate: "2017-09-07",
		Title:       title,
		TraceID:     "2effer334d",
	}

	if dataType == DatasetDataType {
		popType := models.PopulationType{}
		dimensions := []models.Dimension{}

		fmt.Println("Type the population type Name")
		fmt.Printf("$ ")
		scanner.Scan()
		popType.Name = scanner.Text()

		fmt.Println("Type the population type Label")
		fmt.Printf("$ ")
		scanner.Scan()
		popType.Label = scanner.Text()

		for {
			fmt.Println("Add dimension? [Yy] to confirm")
			fmt.Printf("$ ")
			scanner.Scan()
			if strings.ToLower(scanner.Text()) != "y" {
				break
			}

			dim := models.Dimension{}

			fmt.Println("Type the dimension Name")
			fmt.Printf("$ ")
			scanner.Scan()
			dim.Name = scanner.Text()

			fmt.Println("Type the dimension Raw Label")
			fmt.Printf("$ ")
			scanner.Scan()
			dim.RawLabel = scanner.Text()

			fmt.Println("Type the dimension Label")
			fmt.Printf("$ ")
			scanner.Scan()
			dim.Label = scanner.Text()

			dimensions = append(dimensions, dim)
		}

		searchDataImport.PopulationType = popType
		searchDataImport.Dimensions = dimensions
	}

	return searchDataImport
}
