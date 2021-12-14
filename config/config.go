package config

import (
	"time"

	"github.com/kelseyhightower/envconfig"
)

// KafkaTLSProtocol is a constant describing the TLS protocol used for kafka
const KafkaTLSProtocol = "TLS"

// Config represents service configuration for dp-search-data-importer
type Config struct {
	BindAddr                   string        `envconfig:"BIND_ADDR"`
	GracefulShutdownTimeout    time.Duration `envconfig:"GRACEFUL_SHUTDOWN_TIMEOUT"`
	HealthCheckInterval        time.Duration `envconfig:"HEALTHCHECK_INTERVAL"`
	HealthCheckCriticalTimeout time.Duration `envconfig:"HEALTHCHECK_CRITICAL_TIMEOUT"`
	KafkaAddr                  []string      `envconfig:"KAFKA_ADDR"`
	KafkaVersion               string        `envconfig:"KAFKA_VERSION"`
	KafkaOffsetOldest          bool          `envconfig:"KAFKA_OFFSET_OLDEST"`
	KafkaNumWorkers            int           `envconfig:"KAFKA_NUM_WORKERS"`
	KafkaSecProtocol           string        `envconfig:"KAFKA_SEC_PROTO"`
	KafkaSecCACerts            string        `envconfig:"KAFKA_SEC_CA_CERTS"`
	KafkaSecClientCert         string        `envconfig:"KAFKA_SEC_CLIENT_CERT"`
	KafkaSecClientKey          string        `envconfig:"KAFKA_SEC_CLIENT_KEY"          json:"-"`
	KafkaSecSkipVerify         bool          `envconfig:"KAFKA_SEC_SKIP_VERIFY"`
	PublishedContentGroup      string        `envconfig:"KAFKA_PUBLISHED_CONTENT_GROUP"`
	PublishedContentTopic      string        `envconfig:"KAFKA_PUBLISHED_CONTENT_TOPIC"`
	BatchSize                  int           `envconfig:"BATCH_SIZE"`
	BatchWaitTime              time.Duration `envconfig:"BATCH_WAIT_TIME"`
	ElasticSearchAPIURL        string        `envconfig:"ELASTIC_SEARCH_URL"`
	AwsRegion                  string        `envconfig:"AWS_REGION"`
	AwsService                 string        `envconfig:"AWS_SERVICE"`
	SignElasticsearchRequests  bool          `envconfig:"SIGN_ELASTICSEARCH_REQUESTS"`
}

var cfg *Config

// Get returns the default config with any modifications through environment
// variables
func Get() (*Config, error) {
	if cfg != nil {
		return cfg, nil
	}

	cfg = &Config{
		BindAddr:                   "localhost:25900",
		GracefulShutdownTimeout:    5 * time.Second,
		HealthCheckInterval:        30 * time.Second,
		HealthCheckCriticalTimeout: 90 * time.Second,
		KafkaAddr:                  []string{"localhost:9092", "localhost:9093", "localhost:9094"},
		KafkaVersion:               "1.0.2",
		KafkaOffsetOldest:          true,
		KafkaNumWorkers:            1,
		KafkaSecProtocol:           "",
		KafkaSecCACerts:            "",
		KafkaSecClientCert:         "",
		KafkaSecClientKey:          "",
		KafkaSecSkipVerify:         false,
		PublishedContentGroup:      "dp-search-data-importer",
		PublishedContentTopic:      "search-data-import",
		BatchSize:                  500,
		BatchWaitTime:              time.Second * 5,
		ElasticSearchAPIURL:        "http://localhost:11200",
		AwsRegion:                  "eu-west-1",
		AwsService:                 "es",
		SignElasticsearchRequests:  false,
	}

	return cfg, envconfig.Process("", cfg)
}
