package service

import (
	"context"
	"net/http"
	"os"

	"github.com/ONSdigital/dp-elasticsearch/v2/awsauth"
	"github.com/ONSdigital/dp-elasticsearch/v2/elasticsearch"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	dpkafka "github.com/ONSdigital/dp-kafka/v2"
	dphttp "github.com/ONSdigital/dp-net/http"
	"github.com/ONSdigital/dp-search-data-importer/config"
	"github.com/ONSdigital/log.go/v2/log"
)

// ExternalServiceList holds the initialiser and initialisation state of external services.
type ExternalServiceList struct {
	HealthCheck   bool
	KafkaConsumer bool
	ElasticSearch bool
	Init          Initialiser
}

// NewServiceList creates a new service list with the provided initialiser
func NewServiceList(initialiser Initialiser) *ExternalServiceList {
	return &ExternalServiceList{
		HealthCheck:   false,
		KafkaConsumer: false,
		ElasticSearch: false,
		Init:          initialiser,
	}
}

// Init implements the Initialiser interface to initialise dependencies
type Init struct{}

// GetHTTPServer creates an http server and sets the Server flag to true
func (e *ExternalServiceList) GetHTTPServer(bindAddr string, router http.Handler) HTTPServer {
	s := e.Init.DoGetHTTPServer(bindAddr, router)
	return s
}

// GetKafkaConsumer creates a Kafka consumer and sets the consumer flag to true
func (e *ExternalServiceList) GetKafkaConsumer(ctx context.Context, cfg *config.Config) (dpkafka.IConsumerGroup, error) {
	consumer, err := e.Init.DoGetKafkaConsumer(ctx, cfg)
	if err != nil {
		return nil, err
	}
	e.KafkaConsumer = true
	return consumer, nil
}

// GetElasticSearchClient creates a ElasticSearchClient and sets the ElasticSearchClient flag to true
func (e *ExternalServiceList) GetElasticSearchClient(ctx context.Context, cfg *config.Config) (*elasticsearch.Client, error) {
	esClient, err := e.Init.DoGetElasticSearchClient(ctx, cfg)
	if err != nil {
		return nil, err
	}
	e.ElasticSearch = true
	return esClient, nil
}

// GetHealthCheck creates a healthcheck with versionInfo and sets teh HealthCheck flag to true
func (e *ExternalServiceList) GetHealthCheck(cfg *config.Config, buildTime, gitCommit, version string) (HealthChecker, error) {
	hc, err := e.Init.DoGetHealthCheck(cfg, buildTime, gitCommit, version)
	if err != nil {
		return nil, err
	}
	e.HealthCheck = true
	return hc, nil
}

// DoGetHTTPServer creates an HTTP Server with the provided bind address and router
func (e *Init) DoGetHTTPServer(bindAddr string, router http.Handler) HTTPServer {
	s := dphttp.NewServer(bindAddr, router)
	s.HandleOSSignals = false
	return s
}

// DoGetKafkaConsumer returns a Kafka Consumer group
func (e *Init) DoGetKafkaConsumer(ctx context.Context, cfg *config.Config) (dpkafka.IConsumerGroup, error) {

	cgChannels := dpkafka.CreateConsumerGroupChannels(cfg.BatchSize)

	kafkaOffset := dpkafka.OffsetNewest
	if cfg.KafkaOffsetOldest {
		kafkaOffset = dpkafka.OffsetOldest
	}
	cgConfig := &dpkafka.ConsumerGroupConfig{
		KafkaVersion: &cfg.KafkaVersion,
		Offset:       &kafkaOffset,
	}

	kafkaConsumer, err := dpkafka.NewConsumerGroup(
		ctx,
		cfg.KafkaAddr,
		cfg.PublishedContentTopic,
		cfg.PublishedContentGroup,
		cgChannels,
		cgConfig,
	)
	if err != nil {
		return nil, err
	}

	return kafkaConsumer, nil
}

// DoGetHealthCheck creates a healthcheck with versionInfo
func (e *Init) DoGetHealthCheck(cfg *config.Config, buildTime, gitCommit, version string) (HealthChecker, error) {
	versionInfo, err := healthcheck.NewVersionInfo(buildTime, gitCommit, version)
	if err != nil {
		return nil, err
	}
	hc := healthcheck.New(versionInfo, cfg.HealthCheckCriticalTimeout, cfg.HealthCheckInterval)
	return &hc, nil
}

// DoGetElasticSearchClient returns a Elastic Search Client
func (e *Init) DoGetElasticSearchClient(ctx context.Context, cfg *config.Config) (*elasticsearch.Client, error) {

	elasticHTTPClient := dphttp.NewClient()

	if cfg.SignElasticsearchRequests {
		awsSigner, err := createAWSSigner(ctx)
		if err != nil {
			log.Error(ctx, "Error getting aws signer", err)
			return nil, err
		}
		esClientWithAwsSigner := elasticsearch.NewClientWithHTTPClientAndAwsSigner(
			cfg.ElasticSearchAPIURL, awsSigner, cfg.SignElasticsearchRequests, elasticHTTPClient)

		log.Info(ctx, "returning esClientWithAwsSigner")
		return esClientWithAwsSigner, nil
	}

	elasticSearchClient := elasticsearch.NewClientWithHTTPClient(
		cfg.ElasticSearchAPIURL, false, elasticHTTPClient)

	log.Info(ctx, "returning esClientWithNonAwsSigner")
	return elasticSearchClient, nil
}

func createAWSSigner(ctx context.Context) (*awsauth.Signer, error) {
	// Get Config
	cfg, err := config.Get()
	if err != nil {
		log.Fatal(ctx, "error getting config", err)
		os.Exit(1)
	}

	return awsauth.NewAwsSigner(
		cfg.AwsAccessKeyId,
		cfg.AwsSecretAccessKey,
		cfg.AwsRegion,
		cfg.AwsService)
}
