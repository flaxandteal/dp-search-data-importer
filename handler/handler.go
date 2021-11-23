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

	t := transform.NewTransformer()

	go func() {

		defer func() {
			<-semaphore
			syncWaitGroup.Done()
		}()

		log.Info(ctx, "go routine for inserting into ES starts")

		var bulk []byte

		i := 0
		for i < len(events) {

			if events[i].Title == "" {
				log.Info(ctx, "No title for inbound event, no transformation possible")
				continue // break here
			}

			esmodel := t.TransformEventModelToEsModel(events[i])

			if esmodel != nil {
				b, err := json.Marshal(esmodel)
				if err != nil {
					log.Fatal(ctx, "error marshal to json", err)
					return
				}

				bulk = append(bulk, []byte("{ \"create\": { \"_id\": \""+esmodel.Title+"\" } }\n")...)
				bulk = append(bulk, b...)
				bulk = append(bulk, []byte("\n")...)
			}
			i++
		}

		//@TODO  decision on handle failed updates to elasticsearch later
		_, _, err := bh.esClient.BulkUpdate(ctx, esDestIndex, esDestURL, bulk)
		if err != nil {
			log.Error(ctx, "error in response from elasticsearch", err)
			return
		}

		log.Info(ctx, "go routine for inserting into ES ends with insertChannel")
	}()

	return nil
}
