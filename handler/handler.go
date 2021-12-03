package handler

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/ONSdigital/dp-search-data-importer/event"
	"github.com/ONSdigital/dp-search-data-importer/models"
	"github.com/ONSdigital/dp-search-data-importer/transform"
	"github.com/ONSdigital/log.go/v2/log"

	dpelasticsearch "github.com/ONSdigital/dp-elasticsearch/v2/elasticsearch"
)

const esDestIndex = "ons"

var _ event.Handler = (*BatchHandler)(nil)

var (
	syncWaitGroup sync.WaitGroup
	semaphore     = make(chan int, 5)
)

type bulk []byte

type esBulkResponse struct {
	Took   int                  `json:"took"`
	Errors bool                 `json:"errors"`
	Items  []esBulkItemResponse `json:"items"`
}

type esBulkItemResponse map[string]esBulkItemResponseData

type esBulkItemResponseData struct {
	Index  string `json:"_index"`
	ID     string `json:"_id"`
	Status int    `json:"status"`
	Error  string `json:"error"`
}

// BatchHandler handles batches of SearchDataImportModel events that contain CSV row data.
type BatchHandler struct {
	esClient *dpelasticsearch.Client
}

// NewBatchHandler returns a BatchHandler.
func NewBatchHandler(esClient *dpelasticsearch.Client) *BatchHandler {

	return &BatchHandler{
		esClient: esClient,
	}
}

// Handle the given slice of SearchDataImport Model.
func (batchHandler BatchHandler) Handle(ctx context.Context, url string, events []*models.SearchDataImportModel) error {
	log.Info(ctx, "events handler called")

	//no events receeived. Nothing more to do.
	if len(events) == 0 {
		log.Info(ctx, "there are no events to handle")
		return nil
	}
	log.Info(ctx, "Events received ", log.Data{
		"events received": len(events),
	})

	// This will block if we've reached our concurrency limit (sem buffer size)
	err := batchHandler.SendToES(ctx, url, events)
	if err != nil {
		log.Fatal(ctx, "failed to send event to Elastic Search", err)
		return err
	}

	log.Info(ctx, "event successfully handled")
	return nil
}

func (bh BatchHandler) SendToES(ctx context.Context, esDestURL string, events []*models.SearchDataImportModel) error {

	// Wait on semaphore if we've reached our concurrency limit
	syncWaitGroup.Add(1)
	semaphore <- 1

	go func() {

		defer func() {
			<-semaphore
			syncWaitGroup.Done()
		}()

		log.Info(ctx, "go routine for inserting into ES starts")
		documentList := make(map[string]models.SearchDataImportModel)

		var bulkcreate bulk
		i := 0

		for i < len(events) {

			if events[i].Title == "" {
				log.Info(ctx, "No title for inbound event, no transformation possible")
				continue // break here
			}

			documentList[events[i].Title] = *events[i]

			if err := bulkcreate.prepareBulkRequestBody(ctx, events[i], "create"); err != nil {
				log.Error(ctx, "", err, log.Data{})
			}
			i++
		}

		jsonResponse, _, err := bh.esClient.BulkUpdate(ctx, esDestIndex, esDestURL, bulkcreate)
		if err != nil {
			log.Error(ctx, "error in response from elasticsearch for bulkcreate", err)
			return
		}

		var bulkRes esBulkResponse
		if err := json.Unmarshal(jsonResponse, &bulkRes); err != nil {
			log.Error(ctx, "error unmarshaling json", err)
		}

		if bulkRes.Errors {
			var bulkupdate bulk
			for _, r := range bulkRes.Items {
				if r["create"].Status == 409 {
					event := documentList[r["create"].ID]
					if err = bulkupdate.prepareBulkRequestBody(ctx, &event, "update"); err != nil {
						log.Error(ctx, "error in update the bulk loader", err)
						return
					}
				} else {
					//logs for all failed transactions with different status than 409
					log.Error(ctx, "error inserting doc to ES", err,
						log.Data{
							"response status": r["create"].Status,
							"err":             err,
						})
					return
				}
			}
			_, _, err := bh.esClient.BulkUpdate(ctx, esDestIndex, esDestURL, bulkupdate)
			if err != nil {
				log.Error(ctx, "error in response from elasticsearch for bulkupload", err)
				return
			}
		}
	}()

	syncWaitGroup.Wait()
	log.Info(ctx, "go routine for inserting into ES ends")

	return nil
}

func (bulkbody bulk) prepareBulkRequestBody(ctx context.Context, event *models.SearchDataImportModel, method string) error {

	t := transform.NewTransformer()
	esmodel := t.TransformEventModelToEsModel(event)

	if esmodel != nil {
		b, err := json.Marshal(esmodel)
		if err != nil {
			log.Fatal(ctx, "error marshal to json", err)
			return err
		}

		bulkbody = append(bulkbody, []byte("{ \""+method+"\": { \"_id\": \""+esmodel.Title+"\" } }\n")...)
		bulkbody = append(bulkbody, b...)
		bulkbody = append(bulkbody, []byte("\n")...)
	}

	return nil

}
