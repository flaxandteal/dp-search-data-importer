package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/ONSdigital/dp-search-data-importer/config"
	"github.com/ONSdigital/dp-search-data-importer/esclient"
	"github.com/ONSdigital/dp-search-data-importer/event"
	"github.com/ONSdigital/dp-search-data-importer/models"
	"github.com/ONSdigital/dp-search-data-importer/transform"
	"github.com/ONSdigital/log.go/v2/log"
)

var _ event.Handler = (*BatchHandler)(nil)

var (
	syncWaitGroup sync.WaitGroup

	countChannel  = make(chan int)
	insertChannel = make(chan int)
	skipChannel   = make(chan int)
	semaphore     = make(chan int, 5)
)

// BatchHandler handles batches of SearchDataImportModel events that contain CSV row data.
type BatchHandler struct {
	ElasticSearchCli esclient.Client
}

// NewBatchHandler returns a BatchHandler.
func NewBatchHandler(ElasticSearchCli esclient.Client) *BatchHandler {

	return &BatchHandler{
		ElasticSearchCli: ElasticSearchCli,
	}
}

type esBulkResponse struct {
	Took   int                  `json:"took"`
	Errors bool                 `json:"errors"`
	Items  []esBulkItemResponse `json:"items"`
}

type esBulkItemResponse map[string]esBulkItemResponseData

type esBulkItemResponseData struct {
	Index  string `json:"_index"`
	Status int    `json:"status"`
	Error  string `json:"error"`
}

// Handle the given slice of SearchDataImport Model.
func (batchHandler BatchHandler) Handle(ctx context.Context, cfg *config.Config, events []*models.SearchDataImportModel) error {
	log.Info(ctx, "events handler called")

	//no events receeived. Nothing more to do.
	if len(events) == 0 {
		log.Info(ctx, "there are no events to handle")
		return nil
	}
	log.Info(ctx, "Events received ", log.Data{
		"events received": len(events),
	})
	go status(ctx)

	// This will block if we've reached our concurrency limit (sem buffer size)
	err := batchHandler.SendToES(ctx, cfg, events)
	if err != nil {
		log.Fatal(ctx, "failed to send event to Elastic Search", err)
		return err
	}

	time.Sleep(4 * time.Second)
	syncWaitGroup.Wait()

	log.Info(ctx, "event successfully handled")
	return nil
}

func (bh BatchHandler) SendToES(ctx context.Context, cfg *config.Config, events []*models.SearchDataImportModel) error {

	// Wait on semaphore if we've reached our concurrency limit
	syncWaitGroup.Add(1)
	semaphore <- 1

	esDestURL := cfg.ElasticSearchAPIURL
	esIndex := "esIndex"
	esDestType := "docType"

	esDestIndex := fmt.Sprintf("%s/%s", esIndex, esDestType)
	log.Info(ctx, "esDestIndex ", log.Data{"esDestIndex": esDestIndex})

	t := transform.NewTransformer()

	go func() {

		defer func() {
			<-semaphore
			syncWaitGroup.Done()
		}()

		log.Info(ctx, "go routine for inserting into ES starts")

		countChannel <- len(events)
		target := len(events)
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
			} else {
				skipChannel <- 1
				target--
			}
			i++
		}

		b, err := bh.ElasticSearchCli.SubmitBulkToES(ctx, cfg, esDestIndex, esDestURL, bulk)
		if err != nil {
			log.Fatal(ctx, "error in submitting bulk in ES", err)
			return
		}

		var bulkRes esBulkResponse
		if err := json.Unmarshal(b, &bulkRes); err != nil {
			log.Info(ctx, "error unmarshaling json", log.Data{
				"actual response": b,
				"err":             err,
			})
			log.Error(ctx, "error unmarshaling json", err)
		}

		//TODO : handle failed updates to elasticsearch. The plan will be to re-queue these failed events
		if bulkRes.Errors {
			for _, r := range bulkRes.Items {
				if r["create"].Status != 201 {
					log.Error(ctx, "error inserting doc to ES", err)
				}
			}
		}
		insertChannel <- target

		log.Info(ctx, "go routine for inserting into ES ends with insertChannel")

	}()

	return nil
}

func status(ctx context.Context) {
	var (
		rpsCounter  = 0
		insCounter  = 0
		skipCounter = 0
		reqTotal    = 0
		insTotal    = 0
		skipTotal   = 0
	)

	t := time.NewTicker(time.Second)

	for {
		select {
		case n := <-skipChannel:
			skipCounter += n
			skipTotal += n
		case n := <-countChannel:
			rpsCounter += n
			reqTotal += n
		case n := <-insertChannel:
			insCounter += n
			insTotal += n
		case <-t.C:
			logData := log.Data{
				"Read":        reqTotal,
				"Written":     insTotal,
				"skipTotal":   skipTotal,
				"rpsCounter":  rpsCounter,
				"insCounter":  insCounter,
				"skipCounter": skipCounter,
			}
			log.Info(ctx, "Elastic Search Summary", log.INFO, logData)
			rpsCounter = 0
			insCounter = 0
			skipCounter = 0
		}
	}
}
