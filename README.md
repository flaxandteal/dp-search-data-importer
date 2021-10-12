dp-search-data-importer
================
Service to store searchable content into elasticsearch

### Getting started

* Run `make debug`

The service runs in the background consuming messages from Kafka.
An example event can be created using the helper script, `make produce`.

### Dependencies

* Requires running…
  * [kafka](https://github.com/ONSdigital/dp/blob/main/guides/INSTALLING.md#prerequisites)
* No further dependencies other than those defined in `go.mod`

### Configuration

| Environment variable          | Default                           | Description
| ----------------------------  | --------------------------------- | -----------
| BIND_ADDR                     | localhost:25900                   | The host and port to bind to
| GRACEFUL_SHUTDOWN_TIMEOUT     | 5s                                | The graceful shutdown timeout in seconds (`time.Duration` format)
| HEALTHCHECK_INTERVAL          | 30s                               | Time between self-healthchecks (`time.Duration` format)
| HEALTHCHECK_CRITICAL_TIMEOUT  | 90s                               | Time to wait until an unhealthy dependent propagates its state to make this app unhealthy (`time.Duration` format)
| KAFKA_ADDR                    | "localhost:9092"                  | The address of Kafka (accepts list)
| KAFKA_OFFSET_OLDEST           | true                              | Start processing Kafka messages in order from the oldest in the queue
| KAFKA_NUM_WORKERS             | 1                                 | The maximum number of parallel kafka consumers
| KAFKA_SEC_PROTO               | _unset_   (only `TLS`)            | if set to `TLS`, kafka connections will use TLS
| KAFKA_SEC_CLIENT_KEY          | _unset_                           | PEM [2] for the client key (optional, used for client auth) [1]
| KAFKA_SEC_CLIENT_CERT         | _unset_                           | PEM [2] for the client certificate (optional, used for client auth) [1]
| KAFKA_SEC_CA_CERTS            | _unset_                           | PEM [2] of CA cert chain if using private CA for the server cert [1]
| KAFKA_SEC_SKIP_VERIFY         | false                             | ignore server certificate issues if set to `true` [1]
| KAFKA_PUBLISHED_CONTENT_GROUP | dp-search-data-import             | The consumer group this application to consume ImageUploaded messages
| KAFKA_PUBLISHED_CONTENT_TOPIC | search-data-import                | The name of the topic to consume messages from

### Healthcheck

 The `/health` endpoint returns the current status of the service. Dependent services are health checked on an interval defined by the `HEALTHCHECK_INTERVAL` environment variable.

 On a development machine a request to the health check endpoint can be made by:

 `curl localhost:25900/health`

**Notes:**

1. For more info, see the [kafka TLS examples documentation](https://github.com/ONSdigital/dp-kafka/tree/main/examples#tls)


### Contributing

See [CONTRIBUTING](CONTRIBUTING.md) for details.

### License

Copyright © 2021, Office for National Statistics (https://www.ons.gov.uk)

Released under MIT license, see [LICENSE](LICENSE.md) for details.

