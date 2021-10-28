package service_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/ONSdigital/dp-elasticsearch/v2/elasticsearch"
	"github.com/ONSdigital/dp-healthcheck/healthcheck"
	kafka "github.com/ONSdigital/dp-kafka/v2"
	"github.com/ONSdigital/dp-kafka/v2/kafkatest"
	dphttp "github.com/ONSdigital/dp-net/http"
	"github.com/ONSdigital/dp-search-data-importer/config"
	"github.com/ONSdigital/dp-search-data-importer/service"
	serviceMock "github.com/ONSdigital/dp-search-data-importer/service/mock"
	"github.com/pkg/errors"
	. "github.com/smartystreets/goconvey/convey"
)

var (
	ctx           = context.Background()
	testBuildTime = "BuildTime"
	testGitCommit = "GitCommit"
	testVersion   = "Version"

	errElasticSearch = errors.New("Elastic search error")
	errKafkaConsumer = errors.New("Kafka consumer error")
	errHealthcheck   = errors.New("healthCheck error")

	funcDoGetKafkaConsumerErr = func(ctx context.Context, cfg *config.Config) (kafka.IConsumerGroup, error) {
		return nil, errKafkaConsumer
	}

	funcDoGetHealthcheckErr = func(cfg *config.Config, buildTime string, gitCommit string, version string) (service.HealthChecker, error) {
		return nil, errHealthcheck
	}

	funcDoGetHTTPServerNil = func(bindAddr string, router http.Handler) service.HTTPServer {
		return nil
	}

	funDoGetElasticSearchClientErr = func(ctx context.Context, cfg *config.Config) (*elasticsearch.Client, error) {
		return nil, errElasticSearch
	}

	mockHttpClient = &dphttp.ClienterMock{
		DoFunc: doFuncWithValidResponse,
	}

	doFuncWithValidResponse = func(ctx context.Context, req *http.Request) (*http.Response, error) {
		return testResponse("testResponse"), nil
	}
)

func TestRun(t *testing.T) {

	Convey("Having a set of mocked dependencies", t, func() {

		// elasticSearchMock := &dphttp.ClienterMock{
		// 	DoFunc: func(ctx context.Context, req *http.Request) (*http.Response, error) { return nil, nil },
		// }

		// elasticSearchMock := esclient.NewClient(nil, "testurl.co.uk", mockHttpClient, false)

		// The below line is useful if we define interface in our own esclient package
		// But if we need to rely on dp-elastic-search then  need to find the mock for elastic-search

		// elasticSearchMock := &mock.ElasticSearcherMock{
		// 	SearchFunc: func(ctx context.Context, index string, docType string, request []byte) ([]byte, error) {
		// 		return []byte(`{"dummy":"response"`), nil
		// 	},
		// }

		consumerMock := &kafkatest.IConsumerGroupMock{
			CheckerFunc:  func(ctx context.Context, state *healthcheck.CheckState) error { return nil },
			ChannelsFunc: func() *kafka.ConsumerGroupChannels { return &kafka.ConsumerGroupChannels{} },
		}

		hcMock := &serviceMock.HealthCheckerMock{
			AddCheckFunc: func(name string, checker healthcheck.Checker) error { return nil },
			StartFunc:    func(ctx context.Context) {},
		}

		serverWg := &sync.WaitGroup{}
		serverMock := &serviceMock.HTTPServerMock{
			ListenAndServeFunc: func() error {
				serverWg.Done()
				return nil
			},
		}

		funcElasticSearchMockOk := func(ctx context.Context, cfg *config.Config) (*elasticsearch.Client, error) {
			return nil, nil
		}

		funcDoGetKafkaConsumerOk := func(ctx context.Context, cfg *config.Config) (kafka.IConsumerGroup, error) {
			return consumerMock, nil
		}

		funcDoGetHealthcheckOk := func(cfg *config.Config, buildTime string, gitCommit string, version string) (service.HealthChecker, error) {
			return hcMock, nil
		}

		funcDoGetHTTPServer := func(bindAddr string, router http.Handler) service.HTTPServer {
			return serverMock
		}

		Convey("Given that initialising Kafka consumer returns an error", func() {
			initMock := &serviceMock.InitialiserMock{
				DoGetHTTPServerFunc:          funcDoGetHTTPServerNil,
				DoGetKafkaConsumerFunc:       funcDoGetKafkaConsumerErr,
				DoGetElasticSearchClientFunc: funDoGetElasticSearchClientErr,
			}
			svcErrors := make(chan error, 1)
			svcList := service.NewServiceList(initMock)
			_, err := service.Run(ctx, svcList, testBuildTime, testGitCommit, testVersion, svcErrors)

			Convey("Then service Run fails with the same error and the flag is not set", func() {
				So(err, ShouldResemble, errKafkaConsumer)
				So(svcList.ElasticSearch, ShouldBeFalse)
				So(svcList.KafkaConsumer, ShouldBeFalse)
				So(svcList.HealthCheck, ShouldBeFalse)
			})
		})

		Convey("Given that initialising healthcheck returns an error", func() {
			initMock := &serviceMock.InitialiserMock{
				DoGetHTTPServerFunc:          funcDoGetHTTPServerNil,
				DoGetHealthCheckFunc:         funcDoGetHealthcheckErr,
				DoGetKafkaConsumerFunc:       funcDoGetKafkaConsumerOk,
				DoGetElasticSearchClientFunc: funcElasticSearchMockOk,
			}
			svcErrors := make(chan error, 1)
			svcList := service.NewServiceList(initMock)
			_, err := service.Run(ctx, svcList, testBuildTime, testGitCommit, testVersion, svcErrors)

			Convey("Then service Run fails with the same error and the flag is not set", func() {
				So(err, ShouldResemble, errHealthcheck)
				So(svcList.KafkaConsumer, ShouldBeTrue)
				So(svcList.HealthCheck, ShouldBeFalse)
			})
		})

		Convey("Given that all dependencies are successfully initialised", func() {

			initMock := &serviceMock.InitialiserMock{
				DoGetHTTPServerFunc:          funcDoGetHTTPServer,
				DoGetHealthCheckFunc:         funcDoGetHealthcheckOk,
				DoGetKafkaConsumerFunc:       funcDoGetKafkaConsumerOk,
				DoGetElasticSearchClientFunc: funcElasticSearchMockOk,
			}
			svcErrors := make(chan error, 1)
			svcList := service.NewServiceList(initMock)
			serverWg.Add(1)
			_, err := service.Run(ctx, svcList, testBuildTime, testGitCommit, testVersion, svcErrors)

			Convey("Then service Run succeeds and all the flags are set", func() {
				So(err, ShouldBeNil)
				So(svcList.KafkaConsumer, ShouldBeTrue)
				So(svcList.HealthCheck, ShouldBeTrue)
			})

			Convey("The checkers are registered and the healthcheck and http server started", func() {
				So(len(hcMock.AddCheckCalls()), ShouldEqual, 2)
				So(hcMock.AddCheckCalls()[0].Name, ShouldResemble, "Elasticsearch")
				So(hcMock.AddCheckCalls()[1].Name, ShouldResemble, "Kafka consumer")
				So(len(initMock.DoGetHTTPServerCalls()), ShouldEqual, 1)
				So(initMock.DoGetHTTPServerCalls()[0].BindAddr, ShouldEqual, "localhost:25900")
				So(len(hcMock.StartCalls()), ShouldEqual, 1)
				serverWg.Wait() // Wait for HTTP server go-routine to finish
				So(len(serverMock.ListenAndServeCalls()), ShouldEqual, 1)
			})
		})

		Convey("Given that Checkers cannot be registered", func() {

			errAddheckFail := errors.New("Error(s) registering checkers for healthcheck")
			hcMockAddFail := &serviceMock.HealthCheckerMock{
				AddCheckFunc: func(name string, checker healthcheck.Checker) error { return errAddheckFail },
				StartFunc:    func(ctx context.Context) {},
			}

			initMock := &serviceMock.InitialiserMock{
				DoGetHTTPServerFunc: funcDoGetHTTPServerNil,
				DoGetHealthCheckFunc: func(cfg *config.Config, buildTime string, gitCommit string, version string) (service.HealthChecker, error) {
					return hcMockAddFail, nil
				},
				DoGetKafkaConsumerFunc:       funcDoGetKafkaConsumerOk,
				DoGetElasticSearchClientFunc: funcElasticSearchMockOk,
			}
			svcErrors := make(chan error, 1)
			svcList := service.NewServiceList(initMock)
			_, err := service.Run(ctx, svcList, testBuildTime, testGitCommit, testVersion, svcErrors)

			Convey("Then service Run fails, but all checks try to register", func() {
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldResemble, fmt.Sprintf("unable to register checkers: %s", errAddheckFail.Error()))
				So(svcList.HealthCheck, ShouldBeTrue)
				So(svcList.KafkaConsumer, ShouldBeTrue)
				So(len(hcMockAddFail.AddCheckCalls()), ShouldEqual, 2)
				So(hcMockAddFail.AddCheckCalls()[0].Name, ShouldResemble, "Elasticsearch")
				So(hcMockAddFail.AddCheckCalls()[1].Name, ShouldResemble, "Kafka consumer")
			})
		})
	})
}

func TestClose(t *testing.T) {

	Convey("Having a correctly initialised service", t, func() {

		hcStopped := false

		funcElasticSearchMockOk := func(ctx context.Context, cfg *config.Config) (*elasticsearch.Client, error) {
			return nil, nil
		}

		consumerMock := &kafkatest.IConsumerGroupMock{
			StopListeningToConsumerFunc: func(ctx context.Context) error { return nil },
			CloseFunc:                   func(ctx context.Context) error { return nil },
			CheckerFunc:                 func(ctx context.Context, state *healthcheck.CheckState) error { return nil },
			ChannelsFunc:                func() *kafka.ConsumerGroupChannels { return &kafka.ConsumerGroupChannels{} },
		}

		// healthcheck Stop does not depend on any other service being closed/stopped
		hcMock := &serviceMock.HealthCheckerMock{
			AddCheckFunc: func(name string, checker healthcheck.Checker) error { return nil },
			StartFunc:    func(ctx context.Context) {},
			StopFunc:     func() { hcStopped = true },
		}

		// server Shutdown will fail if healthcheck is not stopped
		serverMock := &serviceMock.HTTPServerMock{
			ListenAndServeFunc: func() error { return nil },
			ShutdownFunc: func(ctx context.Context) error {
				if !hcStopped {
					return errors.New("Server stopped before healthcheck")
				}
				return nil
			},
		}

		Convey("Closing the service results in all the dependencies being closed in the expected order", func() {

			initMock := &serviceMock.InitialiserMock{
				DoGetHTTPServerFunc: func(bindAddr string, router http.Handler) service.HTTPServer { return serverMock },
				DoGetHealthCheckFunc: func(cfg *config.Config, buildTime string, gitCommit string, version string) (service.HealthChecker, error) {
					return hcMock, nil
				},
				DoGetKafkaConsumerFunc:       func(ctx context.Context, cfg *config.Config) (kafka.IConsumerGroup, error) { return consumerMock, nil },
				DoGetElasticSearchClientFunc: funcElasticSearchMockOk,
			}

			svcErrors := make(chan error, 1)
			svcList := service.NewServiceList(initMock)
			svc, err := service.Run(ctx, svcList, testBuildTime, testGitCommit, testVersion, svcErrors)
			So(err, ShouldBeNil)

			err = svc.Close(context.Background())
			So(err, ShouldBeNil)
			So(len(consumerMock.StopListeningToConsumerCalls()), ShouldEqual, 1)
			So(len(hcMock.StopCalls()), ShouldEqual, 1)
			So(len(consumerMock.CloseCalls()), ShouldEqual, 1)
			So(len(serverMock.ShutdownCalls()), ShouldEqual, 1)
		})

		Convey("If services fail to stop, the Close operation tries to close all dependencies and returns an error", func() {

			failingserverMock := &serviceMock.HTTPServerMock{
				ListenAndServeFunc: func() error { return nil },
				ShutdownFunc: func(ctx context.Context) error {
					return errors.New("Failed to stop http server")
				},
			}

			initMock := &serviceMock.InitialiserMock{
				DoGetHTTPServerFunc: func(bindAddr string, router http.Handler) service.HTTPServer { return failingserverMock },
				DoGetHealthCheckFunc: func(cfg *config.Config, buildTime string, gitCommit string, version string) (service.HealthChecker, error) {
					return hcMock, nil
				},
				DoGetKafkaConsumerFunc:       func(ctx context.Context, cfg *config.Config) (kafka.IConsumerGroup, error) { return consumerMock, nil },
				DoGetElasticSearchClientFunc: funcElasticSearchMockOk,
			}

			svcErrors := make(chan error, 1)
			svcList := service.NewServiceList(initMock)
			svc, err := service.Run(ctx, svcList, testBuildTime, testGitCommit, testVersion, svcErrors)
			So(err, ShouldBeNil)

			err = svc.Close(context.Background())
			So(err, ShouldNotBeNil)
			So(len(hcMock.StopCalls()), ShouldEqual, 1)
			So(len(failingserverMock.ShutdownCalls()), ShouldEqual, 1)
		})
	})
}

func testResponse(body string) *http.Response {
	recorder := httptest.NewRecorder()
	recorder.WriteString(body)
	return recorder.Result()
}
