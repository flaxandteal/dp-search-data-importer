package config

import (
	"time"

	"github.com/kelseyhightower/envconfig"
)

// KafkaTLSProtocol is a constant describing the TLS protocol used for kafka
const KafkaTLSProtocol = "TLS"

// Config represents service configuration for dp-search-data-importer
type Config struct {
	AwsRegion                  string        `envconfig:"AWS_REGION"`
	AwsService                 string        `envconfig:"AWS_SERVICE"`
	BatchSize                  int           `envconfig:"BATCH_SIZE"`
	BatchWaitTime              time.Duration `envconfig:"BATCH_WAIT_TIME"`
	BerlinAPIURL               string        `envconfig:"BERLIN_API_URL"`
	BindAddr                   string        `envconfig:"BIND_ADDR"`
	ElasticSearchAPIURL        string        `envconfig:"ELASTIC_SEARCH_URL"`
	GracefulShutdownTimeout    time.Duration `envconfig:"GRACEFUL_SHUTDOWN_TIMEOUT"`
	HealthCheckCriticalTimeout time.Duration `envconfig:"HEALTHCHECK_CRITICAL_TIMEOUT"`
	HealthCheckInterval        time.Duration `envconfig:"HEALTHCHECK_INTERVAL"`
	Kafka                      *Kafka
	SignElasticsearchRequests  bool `envconfig:"SIGN_ELASTICSEARCH_REQUESTS"`
	StopConsumingOnUnhealthy   bool `envconfig:"STOP_CONSUMING_ON_UNHEALTHY"`
}

// Kafka contains the config required to connect to Kafka
type Kafka struct {
	Addr                      []string      `envconfig:"KAFKA_ADDR"`
	ConsumerMinBrokersHealthy int           `envconfig:"KAFKA_CONSUMER_MIN_BROKERS_HEALTHY"`
	MaxBytes                  int           `envconfig:"KAFKA_MAX_BYTES"`
	NumWorkers                int           `envconfig:"KAFKA_NUM_WORKERS"`
	OffsetOldest              bool          `envconfig:"KAFKA_OFFSET_OLDEST"`
	ProducerMinBrokersHealthy int           `envconfig:"KAFKA_PRODUCER_MIN_BROKERS_HEALTHY"`
	PublishedContentGroup     string        `envconfig:"KAFKA_PUBLISHED_CONTENT_GROUP"`
	PublishedContentTopic     string        `envconfig:"KAFKA_PUBLISHED_CONTENT_TOPIC"`
	SecCACerts                string        `envconfig:"KAFKA_SEC_CA_CERTS"            json:"-"`
	SecClientCert             string        `envconfig:"KAFKA_SEC_CLIENT_CERT"         json:"-"`
	SecClientKey              string        `envconfig:"KAFKA_SEC_CLIENT_KEY"          json:"-"`
	SecProtocol               string        `envconfig:"KAFKA_SEC_PROTO"`
	SecSkipVerify             bool          `envconfig:"KAFKA_SEC_SKIP_VERIFY"`
	Version                   string        `envconfig:"KAFKA_VERSION"`
	ConsumerBatchSize         int           `envconfig:"KAFKA_CONSUMER_BATCH_SIZE"`
	ConsumerBatchWaitTime     time.Duration `envconfig:"KAFKA_CONSUMER_BATCH_WAIT_TIME"`
}

var cfg *Config

// Get returns the default config with any modifications through environment
// variables
func Get() (*Config, error) {
	if cfg != nil {
		return cfg, nil
	}

	cfg = &Config{
		BerlinAPIURL:               "http://localhost:28900",
		BindAddr:                   "localhost:25900",
		GracefulShutdownTimeout:    5 * time.Second,
		HealthCheckInterval:        30 * time.Second,
		HealthCheckCriticalTimeout: 90 * time.Second,
		BatchSize:                  500,
		BatchWaitTime:              time.Second * 5,
		ElasticSearchAPIURL:        "http://localhost:11200",
		AwsRegion:                  "eu-west-1",
		AwsService:                 "es",
		SignElasticsearchRequests:  false,
		StopConsumingOnUnhealthy:   true,
		Kafka: &Kafka{
			PublishedContentGroup:     "dp-search-data-importer",
			PublishedContentTopic:     "search-data-import",
			Addr:                      []string{"localhost:9092", "localhost:9093", "localhost:9094"},
			Version:                   "1.0.2",
			OffsetOldest:              true,
			NumWorkers:                1,
			SecProtocol:               "",
			SecCACerts:                "",
			SecClientCert:             "",
			SecClientKey:              "",
			SecSkipVerify:             false,
			MaxBytes:                  2000000,
			ConsumerMinBrokersHealthy: 1,
			ProducerMinBrokersHealthy: 1,
			ConsumerBatchSize:         500,
			ConsumerBatchWaitTime:     5 * time.Second,
		},
	}

	return cfg, envconfig.Process("", cfg)
}
