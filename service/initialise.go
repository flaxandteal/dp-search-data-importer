package service

import (
	"context"
	"fmt"
	"net/http"

	dpES "github.com/ONSdigital/dp-elasticsearch/v3"
	dpESClient "github.com/ONSdigital/dp-elasticsearch/v3/client"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	kafka "github.com/ONSdigital/dp-kafka/v3"
	dpawsauth "github.com/ONSdigital/dp-net/v2/awsauth"
	dphttp "github.com/ONSdigital/dp-net/v2/http"
	"github.com/ONSdigital/dp-search-data-importer/config"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/pkg/errors"
)

// GetHTTPServer creates an HTTP Server with the provided bind address and router
var GetHTTPServer = func(bindAddr string, router http.Handler) HTTPServer {
	s := dphttp.NewServer(bindAddr, router)
	s.HandleOSSignals = false
	return s
}

// GetHealthCheck creates a healthcheck with versionInfo
var GetHealthCheck = func(cfg *config.Config, buildTime, gitCommit, version string) (HealthChecker, error) {
	versionInfo, err := healthcheck.NewVersionInfo(buildTime, gitCommit, version)
	if err != nil {
		return nil, fmt.Errorf("failed to get version info: %w", err)
	}
	hc := healthcheck.New(
		versionInfo,
		cfg.HealthCheckCriticalTimeout,
		cfg.HealthCheckInterval,
	)
	return &hc, nil
}

// GetKafkaConsumer returns a Kafka Consumer group
var GetKafkaConsumer = func(ctx context.Context, cfg *config.Kafka) (kafka.IConsumerGroup, error) {
	if cfg == nil {
		return nil, errors.New("cannot create a kafka consumer without kafka config")
	}
	kafkaOffset := kafka.OffsetNewest
	if cfg.OffsetOldest {
		kafkaOffset = kafka.OffsetOldest
	}
	cgConfig := &kafka.ConsumerGroupConfig{
		BrokerAddrs:       cfg.Addr,
		Topic:             cfg.PublishedContentTopic,
		GroupName:         cfg.PublishedContentGroup,
		MinBrokersHealthy: &cfg.ConsumerMinBrokersHealthy,
		KafkaVersion:      &cfg.Version,
		Offset:            &kafkaOffset,
		BatchSize:         &cfg.ConsumerBatchSize,
		BatchWaitTime:     &cfg.ConsumerBatchWaitTime,
	}
	if cfg.SecProtocol == config.KafkaTLSProtocol {
		cgConfig.SecurityConfig = kafka.GetSecurityConfig(
			cfg.SecCACerts,
			cfg.SecClientCert,
			cfg.SecClientKey,
			cfg.SecSkipVerify,
		)
	}
	return kafka.NewConsumerGroup(ctx, cgConfig)
}

// GetElasticSearchClient returns an Elastic Search Client with the AWS signer if the flag 'SignElasticsearchRequests' is enabled
var GetElasticSearchClient = func(ctx context.Context, cfg *config.Config) (dpESClient.Client, error) {
	esConfig := dpESClient.Config{
		ClientLib: dpESClient.GoElasticV710,
		Address:   cfg.ElasticSearchAPIURL,
	}

	if cfg.SignElasticsearchRequests {
		awsSigner, err := dpawsauth.NewAWSSignerRoundTripper("", "", cfg.AwsRegion, cfg.AwsService)
		if err != nil {
			return nil, fmt.Errorf("failed to create aws v4 signer: %w", err)
		}
		esConfig.Transport = awsSigner
	}

	esClient, esClientErr := dpES.NewClient(esConfig)
	if esClientErr != nil {
		log.Error(ctx, "Failed to create dp-elasticsearch client", esClientErr)
		return nil, esClientErr
	}
	return esClient, nil
}
