package handler_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ONSdigital/dp-api-clients-go/v2/nlp/berlin"
	brlErrs "github.com/ONSdigital/dp-api-clients-go/v2/nlp/berlin/errors"
	brlModels "github.com/ONSdigital/dp-api-clients-go/v2/nlp/berlin/models"
	dpMock "github.com/ONSdigital/dp-elasticsearch/v3/client/mocks"
	kafka "github.com/ONSdigital/dp-kafka/v3"
	"github.com/ONSdigital/dp-kafka/v3/kafkatest"
	"github.com/ONSdigital/dp-search-data-importer/config"
	"github.com/ONSdigital/dp-search-data-importer/handler"
	"github.com/ONSdigital/dp-search-data-importer/models"
	"github.com/ONSdigital/dp-search-data-importer/schema"
	. "github.com/smartystreets/goconvey/convey"
)

var (
	testContext = context.Background()
	indexName   = "ons"
	esDestURL   = "locahost:9999"
	testCfg     = &config.Config{
		ElasticSearchAPIURL: esDestURL,
	}

	expectedEvent1 = &models.SearchDataImport{
		UID:             "uid1",
		DataType:        "anyDataType1",
		JobID:           "",
		SearchIndex:     "ONS",
		CDID:            "",
		DatasetID:       "",
		Keywords:        []string{"anykeyword1"},
		MetaDescription: "",
		Summary:         "",
		ReleaseDate:     "",
		Title:           "anyTitle1",
		TraceID:         "anyTraceID1",
		DateChanges:     []models.ReleaseDateDetails{},
		Cancelled:       false,
		Finalised:       false,
		ProvisionalDate: "",
		Published:       false,
		Survey:          "",
		Language:        "",
		CanonicalTopic:  "",
		PopulationType: models.PopulationType{
			Name:  "pop1",
			Label: "popLbl1",
		},
		Dimensions: []models.Dimension{
			{Name: "dim1", Label: "dimLbl1", RawLabel: "dimRawLbl1"},
		},
	}

	expectedEvent2 = &models.SearchDataImport{
		UID:             "uid2",
		DataType:        "anyDataType2",
		JobID:           "",
		SearchIndex:     "ONS",
		CDID:            "",
		DatasetID:       "",
		Keywords:        []string{"anykeyword2"},
		MetaDescription: "",
		Summary:         "",
		ReleaseDate:     "",
		Title:           "anyTitle2",
		TraceID:         "anyTraceID2",
	}

	testEvents = []*models.SearchDataImport{
		expectedEvent1,
		expectedEvent2,
	}
	mockSuccessESResponseWithNoError = []byte("{\"took\":6,\"errors\":false,\"items\":[{\"create\":{\"_index\":\"ons1637667136829001\",\"_type\":\"_doc\",\"_id\":\"testTitle3\",\"_version\":1,\"result\":\"created\",\"_shards\":{\"total\":2,\"successful\":2,\"failed\":0},\"_seq_no\":0,\"_primary_term\":1,\"status\":201}}]}")
)

func TestHandleWithEventsCreated(t *testing.T) {
	Convey("Given a handler configured with successful es updates for all two events is success", t, func() {
		elasticSearchMock := &dpMock.ClientMock{
			BulkUpdateFunc: func(ctx context.Context, indexName string, url string, settings []byte) ([]byte, error) {
				return mockSuccessESResponseWithNoError, nil
			},
		}

		brlClient := mockBerlinClient()
		batchHandler := handler.NewBatchHandler(elasticSearchMock, testCfg, brlClient)

		Convey("When handle is called with no error", func() {
			err := batchHandler.Handle(testContext, createTestBatch(testEvents))

			Convey("Then the error is nil and it performed upsert action to the expected index", func() {
				So(err, ShouldBeNil)
				So(elasticSearchMock.BulkUpdateCalls(), ShouldHaveLength, 1)
				So(elasticSearchMock.BulkUpdateCalls()[0].IndexName, ShouldEqual, indexName)
				So(elasticSearchMock.BulkUpdateCalls()[0].URL, ShouldEqual, esDestURL)
				So(elasticSearchMock.BulkUpdateCalls()[0].Settings, ShouldNotBeEmpty)
			})
		})
	})
}

func TestHandleWithEventsUpdated(t *testing.T) {
	Convey("Given a handler configured with sucessful es updates for two events with one create error", t, func() {
		elasticSearchMock := &dpMock.ClientMock{
			BulkUpdateFunc: func(ctx context.Context, indexName string, url string, settings []byte) ([]byte, error) {
				return mockSuccessESResponseWithNoError, nil
			},
		}

		brlClient := mockBerlinClient()
		batchHandler := handler.NewBatchHandler(elasticSearchMock, testCfg, brlClient)

		Convey("When handle is called", func() {
			err := batchHandler.Handle(testContext, createTestBatch(testEvents))

			Convey("Then the error is nil and it performed upsert action to the expected index", func() {
				So(err, ShouldBeNil)
				So(elasticSearchMock.BulkUpdateCalls(), ShouldHaveLength, 1)
				So(elasticSearchMock.BulkUpdateCalls()[0].IndexName, ShouldEqual, indexName)
				So(elasticSearchMock.BulkUpdateCalls()[0].URL, ShouldEqual, esDestURL)
				So(elasticSearchMock.BulkUpdateCalls()[0].Settings, ShouldNotBeEmpty)
			})
		})
	})
}

func TestHandleWithInternalServerESResponse(t *testing.T) {
	Convey("Given a handler configured with other failed es create request", t, func() {
		elasticSearchMock := &dpMock.ClientMock{
			BulkUpdateFunc: func(ctx context.Context, indexName string, url string, settings []byte) ([]byte, error) {
				return nil, errors.New("unexpected status code from api")
			},
		}

		brlClient := mockBerlinClient()
		batchHandler := handler.NewBatchHandler(elasticSearchMock, testCfg, brlClient)

		Convey("When handle is called", func() {
			err := batchHandler.Handle(testContext, createTestBatch(testEvents))

			Convey("And the error is not nil while performing upsert action", func() {
				So(err, ShouldResemble, errors.New("unexpected status code from api"))
			})
		})
	})
}

func createTestBatch(events []*models.SearchDataImport) []kafka.Message {
	batch := make([]kafka.Message, len(events))
	for i, s := range events {
		e, err := schema.SearchDataImportEvent.Marshal(s)
		So(err, ShouldBeNil)
		msg, err := kafkatest.NewMessage(e, int64(i))
		So(err, ShouldBeNil)
		batch[i] = msg
	}
	return batch
}

func mockBerlinClient() berlin.ClienterMock {
	return berlin.ClienterMock{
		GetBerlinFunc: func(ctx context.Context, options berlin.Options) (*brlModels.Berlin, brlErrs.Error) {
			return &brlModels.Berlin{
				Matches: []brlModels.Matches{
					brlModels.Matches{
						Subdivision: []string{"subdiv1", "subdiv2"},
					},
				},
			}, nil
		},
	}
}
