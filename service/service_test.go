package service_test

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	dpESClient "github.com/ONSdigital/dp-elasticsearch/v3/client"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	kafka "github.com/ONSdigital/dp-kafka/v3"
	"github.com/ONSdigital/dp-kafka/v3/kafkatest"
	"github.com/ONSdigital/dp-search-data-importer/config"
	"github.com/ONSdigital/dp-search-data-importer/service"
	"github.com/ONSdigital/dp-search-data-importer/service/mock"
	"github.com/pkg/errors"

	. "github.com/smartystreets/goconvey/convey"
)

var (
	ctx           = context.Background()
	testBuildTime = "BuildTime"
	testGitCommit = "GitCommit"
	testVersion   = "Version"
	testChecks    = map[string]*healthcheck.Check{
		"Zebedee client":    {},
		"DatasetAPI client": {},
		"Kafka producer":    {},
	}
	errElasticSearch = errors.New("Elastic search error")
	errKafkaConsumer = errors.New("Kafka consumer error")
	errHealthcheck   = errors.New("healthCheck error")
	errServer        = errors.New("HTTP Server error")
	errAddCheck      = errors.New("healthcheck add check error")
)

func TestNew(t *testing.T) {
	Convey("service.New returns a new empty service struct", t, func() {
		srv := service.New()
		So(*srv, ShouldResemble, service.Service{})
	})
}

func TestInit(t *testing.T) {
	Convey("Given a set of mocked dependencies", t, func() {
		cfg, err := config.Get()
		So(err, ShouldBeNil)

		elasticSearchMock := &mock.ElasticSearchMock{}
		service.GetElasticSearchClient = func(ctx context.Context, cfg *config.Config) (dpESClient.Client, error) {
			return elasticSearchMock, nil
		}

		consumerMock := &kafkatest.IConsumerGroupMock{
			RegisterBatchHandlerFunc: func(ctx context.Context, batchHandler kafka.BatchHandler) error {
				return nil
			},
		}
		service.GetKafkaConsumer = func(ctx context.Context, cfg *config.Kafka) (kafka.IConsumerGroup, error) {
			return consumerMock, nil
		}

		subscribedTo := []*healthcheck.Check{}
		hcMock := &mock.HealthCheckerMock{
			AddAndGetCheckFunc: func(name string, checker healthcheck.Checker) (*healthcheck.Check, error) {
				return testChecks[name], nil
			},
			SubscribeFunc: func(s healthcheck.Subscriber, checks ...*healthcheck.Check) {
				subscribedTo = append(subscribedTo, checks...)
			},
		}
		service.GetHealthCheck = func(cfg *config.Config, buildTime, gitCommit, version string) (service.HealthChecker, error) {
			return hcMock, nil
		}

		serverMock := &mock.HTTPServerMock{}
		service.GetHTTPServer = func(bindAddr string, router http.Handler) service.HTTPServer {
			return serverMock
		}

		svc := &service.Service{}

		Convey("Tying to initialise a service without a config returns the expected error", func() {
			err := svc.Init(ctx, nil, testBuildTime, testGitCommit, testVersion)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "nil config passed to service init")
		})

		Convey("Given that creating an elastic search client returns an error", func() {
			service.GetElasticSearchClient = func(ctx context.Context, cfg *config.Config) (dpESClient.Client, error) {
				return nil, errElasticSearch
			}

			Convey("Then service Init fails with the same error and no further initialisations are attempted", func() {
				err := svc.Init(ctx, cfg, testBuildTime, testGitCommit, testVersion)
				So(errors.Unwrap(err), ShouldResemble, errElasticSearch)
				So(svc.Cfg, ShouldResemble, cfg)

				Convey("And no checkers are registered ", func() {
					So(hcMock.AddAndGetCheckCalls(), ShouldHaveLength, 0)
				})
			})
		})

		Convey("Given that initialising Kafka consumer returns an error", func() {
			service.GetKafkaConsumer = func(ctx context.Context, cfg *config.Kafka) (kafka.IConsumerGroup, error) {
				return nil, errKafkaConsumer
			}

			Convey("Then service Init fails with the same error and no further initialisations are attempted", func() {
				err := svc.Init(ctx, cfg, testBuildTime, testGitCommit, testVersion)
				So(errors.Unwrap(err), ShouldResemble, errKafkaConsumer)
				So(svc.Cfg, ShouldResemble, cfg)

				Convey("And no checkers are registered ", func() {
					So(hcMock.AddAndGetCheckCalls(), ShouldHaveLength, 0)
				})
			})
		})

		Convey("Given that Kafka consumer fails to register a batch handler", func() {
			consumerMock.RegisterBatchHandlerFunc = func(ctx context.Context, batchHandler kafka.BatchHandler) error {
				return errKafkaConsumer
			}

			Convey("Then service Init fails with the same error and no further initialisations are attempted", func() {
				err := svc.Init(ctx, cfg, testBuildTime, testGitCommit, testVersion)
				So(errors.Unwrap(err), ShouldResemble, errKafkaConsumer)
				So(svc.Cfg, ShouldResemble, cfg)

				Convey("And no checkers are registered ", func() {
					So(hcMock.AddAndGetCheckCalls(), ShouldHaveLength, 0)
				})
			})
		})

		Convey("Given that initialising healthcheck returns an error", func() {
			service.GetHealthCheck = func(cfg *config.Config, buildTime, gitCommit, version string) (service.HealthChecker, error) {
				return nil, errHealthcheck
			}

			Convey("Then service Init fails with the same error and no further initialisations are attempted", func() {
				err := svc.Init(ctx, cfg, testBuildTime, testGitCommit, testVersion)
				So(errors.Unwrap(err), ShouldResemble, errHealthcheck)
				So(svc.Cfg, ShouldResemble, cfg)
				So(svc.Consumer, ShouldResemble, consumerMock)

				Convey("And no checkers are registered ", func() {
					So(hcMock.AddAndGetCheckCalls(), ShouldHaveLength, 0)
				})
			})
		})

		Convey("Given that Checkers cannot be registered", func() {
			hcMock.AddAndGetCheckFunc = func(name string, checker healthcheck.Checker) (*healthcheck.Check, error) { return nil, errAddCheck }

			Convey("Then service Init fails with the expected error", func() {
				err := svc.Init(ctx, cfg, testBuildTime, testGitCommit, testVersion)
				So(err, ShouldNotBeNil)
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "unable to register checkers: Error(s) registering checkers for healthcheck")
				So(svc.Cfg, ShouldResemble, cfg)
				So(svc.Consumer, ShouldResemble, consumerMock)

				Convey("And all other checkers try to register", func() {
					So(hcMock.AddAndGetCheckCalls(), ShouldHaveLength, 2)
				})
			})
		})

		Convey("Given that all dependencies are successfully initialised", func() {
			Convey("Then service Init succeeds and all dependencies are initialised", func() {
				err := svc.Init(ctx, cfg, testBuildTime, testGitCommit, testVersion)
				So(err, ShouldBeNil)
				So(svc.Cfg, ShouldResemble, cfg)
				So(svc.Server, ShouldEqual, serverMock)
				So(svc.HealthCheck, ShouldResemble, hcMock)
				So(svc.Consumer, ShouldResemble, consumerMock)
				So(svc.EsCli, ShouldResemble, elasticSearchMock)

				Convey("Then all checks are registered", func() {
					So(hcMock.AddAndGetCheckCalls(), ShouldHaveLength, 2)
					So(hcMock.AddAndGetCheckCalls()[0].Name, ShouldResemble, "Elasticsearch")
					So(hcMock.AddAndGetCheckCalls()[1].Name, ShouldResemble, "Kafka consumer")
				})

				Convey("Then kafka consumer subscribes to the expected healthcheck checks", func() {
					So(subscribedTo, ShouldHaveLength, 1)
					So(hcMock.SubscribeCalls(), ShouldHaveLength, 1)
					So(hcMock.SubscribeCalls()[0].Checks, ShouldContain, testChecks["Elasticsearch client"])
				})

				Convey("Then the batch handler is registered to the kafka consumer", func() {
					So(consumerMock.RegisterBatchHandlerCalls(), ShouldHaveLength, 1)
				})
			})
		})
	})
}

func TestStart(t *testing.T) {
	Convey("Having a correctly initialised Service with mocked dependencies", t, func() {
		cfg, err := config.Get()
		So(err, ShouldBeNil)

		consumerMock := &kafkatest.IConsumerGroupMock{
			LogErrorsFunc: func(ctx context.Context) {},
		}

		elasticSearchMock := &mock.ElasticSearchMock{}

		hcMock := &mock.HealthCheckerMock{
			StartFunc: func(ctx context.Context) {},
		}

		serverWg := &sync.WaitGroup{}
		serverMock := &mock.HTTPServerMock{}

		svc := &service.Service{
			Cfg:         cfg,
			Server:      serverMock,
			HealthCheck: hcMock,
			Consumer:    consumerMock,
			EsCli:       elasticSearchMock,
		}

		Convey("When a service with a successful HTTP server is started", func() {
			cfg.StopConsumingOnUnhealthy = true
			serverMock.ListenAndServeFunc = func() error {
				serverWg.Done()
				return nil
			}
			serverWg.Add(1)
			err := svc.Start(ctx, make(chan error, 1))
			So(err, ShouldBeNil)

			Convey("Then healthcheck is started and HTTP server starts listening", func() {
				So(len(hcMock.StartCalls()), ShouldEqual, 1)
				serverWg.Wait() // Wait for HTTP server go-routine to finish
				So(len(serverMock.ListenAndServeCalls()), ShouldEqual, 1)
			})
		})

		Convey("When a service is started with StopConsumingOnUnhealthy disabled", func() {
			cfg.StopConsumingOnUnhealthy = false
			consumerMock.StartFunc = func() error { return nil }
			serverMock.ListenAndServeFunc = func() error { return nil }
			err := svc.Start(ctx, make(chan error, 1))
			So(err, ShouldBeNil)

			Convey("Then the kafka consumer is manually started", func() {
				So(consumerMock.StartCalls(), ShouldHaveLength, 1)
			})
		})

		Convey("When a service is started with StopConsumingOnUnhealthy disabled and the Start func returns an error", func() {
			cfg.StopConsumingOnUnhealthy = false
			consumerMock.StartFunc = func() error { return errKafkaConsumer }
			serverMock.ListenAndServeFunc = func() error { return nil }
			err := svc.Start(ctx, make(chan error, 1))

			Convey("Then the expected error is returned", func() {
				So(consumerMock.StartCalls(), ShouldHaveLength, 1)
				So(err, ShouldNotBeNil)
				So(errors.Unwrap(err), ShouldResemble, errKafkaConsumer)
			})
		})

		Convey("When a service with a failing HTTP server is started", func() {
			cfg.StopConsumingOnUnhealthy = true
			serverMock.ListenAndServeFunc = func() error {
				serverWg.Done()
				return errServer
			}
			errChan := make(chan error, 1)
			serverWg.Add(1)
			err := svc.Start(ctx, errChan)
			So(err, ShouldBeNil)

			Convey("Then HTTP server errors are reported to the provided errors channel", func() {
				rxErr := <-errChan
				So(rxErr.Error(), ShouldResemble, fmt.Sprintf("failure in http listen and serve: %s", errServer.Error()))
			})
		})
	})
}

func TestClose(t *testing.T) {
	Convey("Having a service without initialised dependencies", t, func() {
		cfg := &config.Config{
			GracefulShutdownTimeout: 5 * time.Second,
		}
		svc := service.Service{
			Cfg: cfg,
		}

		Convey("Then the service can be closed without any issue (noop)", func() {
			err := svc.Close(context.Background())
			So(err, ShouldBeNil)
		})
	})

	Convey("Having a service with a kafka consumer that takes more time to stop listening than the graceful shutdown timeout", t, func() {
		cfg := &config.Config{
			GracefulShutdownTimeout: time.Millisecond,
		}
		consumerMock := &kafkatest.IConsumerGroupMock{
			StopAndWaitFunc: func() error {
				time.Sleep(100 * time.Millisecond)
				return nil
			},
		}

		svc := service.Service{
			Cfg:      cfg,
			Consumer: consumerMock,
		}

		Convey("Then the service fails to close due to a timeout error", func() {
			err := svc.Close(context.Background())
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "shutdown timed out: context deadline exceeded")
		})
	})

	Convey("Having a fully initialised service", t, func() {
		cfg := &config.Config{
			GracefulShutdownTimeout: 5 * time.Second,
		}
		consumerMock := &kafkatest.IConsumerGroupMock{}
		hcMock := &mock.HealthCheckerMock{}
		serverMock := &mock.HTTPServerMock{}

		svc := &service.Service{
			Cfg:         cfg,
			Server:      serverMock,
			HealthCheck: hcMock,
			Consumer:    consumerMock,
		}

		Convey("And all mocks can successfully close, if done in the right order", func() {
			hcStopped := false

			consumerMock.StopAndWaitFunc = func() error { return nil }
			consumerMock.CloseFunc = func(ctx context.Context, optFuncs ...kafka.OptFunc) error { return nil }
			hcMock.StopFunc = func() { hcStopped = true }
			serverMock.ShutdownFunc = func(ctx context.Context) error {
				if !hcStopped {
					return fmt.Errorf("server stopped before healthcheck")
				}
				return nil
			}

			Convey("Then the service can be successfully closed", func() {
				err := svc.Close(context.Background())
				So(err, ShouldBeNil)

				Convey("And all the dependencies are closed", func() {
					So(consumerMock.StopAndWaitCalls(), ShouldHaveLength, 1)
					So(hcMock.StopCalls(), ShouldHaveLength, 1)
					So(consumerMock.CloseCalls(), ShouldHaveLength, 1)
					So(serverMock.ShutdownCalls(), ShouldHaveLength, 1)
				})
			})
		})

		Convey("And all mocks fail to close", func() {
			consumerMock.StopAndWaitFunc = func() error { return errKafkaConsumer }
			consumerMock.CloseFunc = func(ctx context.Context, optFuncs ...kafka.OptFunc) error { return errKafkaConsumer }
			hcMock.StopFunc = func() {}
			serverMock.ShutdownFunc = func(ctx context.Context) error { return errServer }

			Convey("Then the service returns the expected error when closed", func() {
				err := svc.Close(context.Background())
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "failed to shutdown gracefully")

				Convey("And all the dependencies are closed", func() {
					So(consumerMock.StopAndWaitCalls(), ShouldHaveLength, 1)
					So(hcMock.StopCalls(), ShouldHaveLength, 1)
					So(consumerMock.CloseCalls(), ShouldHaveLength, 1)
					So(serverMock.ShutdownCalls(), ShouldHaveLength, 1)
				})
			})
		})
	})
}
