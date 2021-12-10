dp-search-data-importer
================
Service to store searchable content into elasticsearch

### Getting started

* Run `make debug`

The service runs in the background consuming messages from Kafka.
An example event can be created using the helper script, `make produce`.

### Dependencies

* Requires running…
	* go v1.17
	* ElasticSearch 7.10
	* [kafka](https://github.com/ONSdigital/dp/blob/main/guides/INSTALLING.md#prerequisites)
* No further dependencies other than those defined in `go.mod`

### Configuration

| Environment variable         | Default                           | Description
| ---------------------------- | --------------------------------- | -----------
| BIND_ADDR                    | localhost:25900                   | The host and port to bind to
| GRACEFUL_SHUTDOWN_TIMEOUT    | 5s                                | The graceful shutdown timeout in seconds (`time.Duration` format)
| HEALTHCHECK_INTERVAL         | 30s                               | Time between self-healthchecks (`time.Duration` format)
| HEALTHCHECK_CRITICAL_TIMEOUT | 90s                               | Time to wait until an unhealthy dependent propagates its state to make this app unhealthy (`time.Duration` format)
| KAFKA_ADDR                   | "localhost:9092"                  | The address of Kafka (accepts list)
| KAFKA_OFFSET_OLDEST          | true                              | Start processing Kafka messages in order from the oldest in the queue
| KAFKA_NUM_WORKERS            | 1                                 | The maximum number of parallel kafka consumers
| PUBLISHED_CONTENT_GROUP      | dp-search-data-importer           | The consumer group this application to consume Uploaded messages
| PUBLISHED_CONTENT_TOPIC      | published-content                 | The name of the topic to consume messages from
| BATCH_SIZE                   | 500                               | The default total number of messages that should be buffered (in batches) before writing to the search engine.
| BATCH_WAIT_TIME              | 5s                                | The default wait time for preparing the batch.
| KAFKA_SEC_PROTO              | unset                             | if set to TLS, kafka connections will use TLS [1]
| KAFKA_SEC_CA_CERTS           | unset                             | CA cert chain for the server cert [1]
| KAFKA_SEC_CLIENT_KEY         | unset                             | PEM for the client key [1]
| KAFKA_SEC_CLIENT_CERT        | unset                             | PEM for the client certificate [1]
| KAFKA_SEC_SKIP_VERIFY        | false                             | ignores server certificate issues if true [1]
| ELASTIC_SEARCH_URL           | "http://localhost:11200"          | The elastic search URL
| AWS_REGION                   | "eu-west-1"                       | The default AWS region to be validated while connecting to elastic search
| AWS_SERVICE                  | "es"                              | The default AWS service to be validated while connecting to elastic search
| SIGN_ELASTICSEARCH_REQUESTS  | false                             | The default configuration for AWS authenticatioin while connecting to elastic search

**Notes:**

1. <a name="notes_1">For more info, see the [kafka TLS examples documentation](https://github.com/ONSdigital/dp-kafka/tree/main/examples#tls)</a>

### Healthcheck

 The `/health` endpoint returns the current status of the service. Dependent services are health checked on an interval defined by the `HEALTHCHECK_INTERVAL` environment variable.

 On a development machine a request to the health check endpoint can be made by:

 `curl localhost:25900/health`

### Contributing

See [CONTRIBUTING](CONTRIBUTING.md) for details.

### License

Copyright © 2021, Office for National Statistics (https://www.ons.gov.uk)

Released under MIT license, see [LICENSE](LICENSE.md) for details.

