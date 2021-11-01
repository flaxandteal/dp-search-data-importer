package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/ONSdigital/dp-search-data-importer/config"
	"github.com/ONSdigital/dp-search-data-importer/esclient"
	"github.com/ONSdigital/dp-search-data-importer/event"
	"github.com/ONSdigital/dp-search-data-importer/models"
	"github.com/ONSdigital/dp-search-data-importer/transform"
	"github.com/ONSdigital/log.go/v2/log"
	"github.com/pkg/errors"

	dpAwsauth "github.com/ONSdigital/dp-elasticsearch/v2/awsauth"
	dphttp "github.com/ONSdigital/dp-net/http"
)

var _ event.Handler = (*BatchHandler)(nil)

var (
	syncWaitGroup sync.WaitGroup

	countChannel  = make(chan int)
	insertChannel = make(chan int)
	skipChannel   = make(chan int)
	semaphore     = make(chan int, 5)
)

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
}

// NewBatchHandler returns a BatchHandler.
func NewBatchHandler() *BatchHandler {

	return &BatchHandler{}
}

// Handle the given slice of SearchDataImport Model.
func (batchHandler BatchHandler) Handle(ctx context.Context, events []*models.SearchDataImportModel) error {
	log.Info(ctx, "events handler called")

	//no events receeived. Nothing more to do.
	if len(events) == 0 {
		log.Info(ctx, "there are no events to handle")
		return nil
	}
	go status()

	// This will block if we've reached our concurrency limit (sem buffer size)
	err := SendToES(ctx, events, len(events))
	if err != nil {
		log.Fatal(ctx, "failed to send event to Elastic Search", err)
	}

	time.Sleep(5 * time.Second)
	syncWaitGroup.Wait()

	log.Info(ctx, "event successfully handled")
	return nil
}

func SendToES(ctx context.Context, events []*models.SearchDataImportModel, length int) error {

	// Wait on semaphore if we've reached our concurrency limit
	syncWaitGroup.Add(1)
	semaphore <- 1

	// Get Config
	cfg, err := config.Get()
	if err != nil {
		log.Fatal(ctx, "error getting config", err)
		return errors.Wrap(err, "unable to retrieve service configuration")
	}

	esDestURL := cfg.ElasticSearchAPIURL
	esDestIndex := "esDestIndex"
	// esDestType  := "docType"

	awsSDKSigner, err := createAWSSigner(ctx)
	if err != nil {
		log.Error(ctx, "error getting awsSDKSigner", err)
		return errors.Wrap(err, "error getting awsSDKSigner")
	}

	t := transform.NewTransformer()
	esHttpClient := dphttp.NewClient()
	elasticsearchcli := esclient.NewClient(awsSDKSigner, esHttpClient, cfg.SignElasticsearchRequests)

	go func() {
		defer func() {
			<-semaphore
			syncWaitGroup.Done()
		}()

		countChannel <- length
		target := length
		var bulk []byte

		i := 0
		for i < length {

			if events[i].Title == "" {
				log.Info(ctx, "No title for inbound event, no transformation possible", log.Data{"title": events[i].Title})
				continue // break here
			}

			//ToDo : title as context
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

		b, err := elasticsearchcli.SubmitBulkToES(ctx, esDestIndex, esDestURL, bulk)
		if err != nil {
			log.Fatal(ctx, "error SubmitBulkToES", err)
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

		if bulkRes.Errors {
			for _, r := range bulkRes.Items {
				if r["create"].Status != 201 {
					log.Fatal(ctx, "error inserting doc to ES", err)
				}
			}
		}
		log.Info(ctx, "3. +++++++++++++++++esDestURL", log.Data{"bulk": string(bulk)})

		insertChannel <- target
	}()

	return nil
}

func createAWSSigner(ctx context.Context) (*dpAwsauth.Signer, error) {
	// Get Config
	cfg, err := config.Get()
	if err != nil {
		log.Fatal(ctx, "error getting config", err)
		os.Exit(1)
	}

	return dpAwsauth.NewAwsSigner(
		cfg.AwsAccessKeyId,
		cfg.AwsSecretAccessKey,
		cfg.AwsRegion,
		cfg.AwsService)
}

// ---------------------------------------------------------------------------

func status() {
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
			fmt.Printf("\nRead: %6d  Written: %6d  Skipped: %6d  |  rps: %6d  ips: %6d  sps: %6d", reqTotal, insTotal, skipTotal, rpsCounter, insCounter, skipCounter)
			rpsCounter = 0
			insCounter = 0
			skipCounter = 0
		}
	}
}

// ------------------------------------------------------------------------------
