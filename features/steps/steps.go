package steps

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/ONSdigital/dp-search-data-importer/models"
	"github.com/ONSdigital/dp-search-data-importer/schema"
	"github.com/cucumber/godog"
	"github.com/google/go-cmp/cmp"
	"github.com/rdumont/assistdog"
)

func (c *Component) RegisterSteps(ctx *godog.ScenarioContext) {
	ctx.Step(`^the service starts`, c.theServiceStarts)
	ctx.Step(`^elasticsearch is healthy`, c.elasticSearchIsHealthy)
	ctx.Step(`^elasticsearch is unhealthy`, c.elasticSearchIsUnhealthy)
	ctx.Step(`^elasticsearch returns the following response for bulk update$`, c.elasticsearchReturns)
	ctx.Step(`^this search-data-import event is queued, to be consumed$`, c.thisSearchDataImportEventIsQueued)
	ctx.Step(`^nothing is sent to elasticsearch`, c.notingSentToElasticSearch)
	ctx.Step(`^this model is sent to elasticsearch$`, c.thisModelIsSentToElasticSearch)
}

// theServiceStarts starts the service under test in a new go-routine
// note that this step should be called only after all dependencies have been setup,
// to prevent any race condition, specially during the first healthcheck iteration.
func (c *Component) theServiceStarts() error {
	return c.startService(c.ctx)
}

// elasticSearchIsHealthy generates a mocked healthy response for elasticsearch healthecheck
func (c *Component) elasticSearchIsHealthy() error {
	const res = `{"cluster_name": "docker-cluster", "status": "green"}`
	c.ElasticSearchAPI.NewHandler().
		Get("/_cluster/health").
		Reply(http.StatusOK).
		BodyString(res)
	return nil
}

// elasticSearchIsUnhealthy generates a mocked unhealthy response for elasticsearch healthecheck
func (c *Component) elasticSearchIsUnhealthy() error {
	const res = `{"cluster_name": "docker-cluster", "status": "red"}`
	c.ElasticSearchAPI.NewHandler().
		Get("/_cluster/health").
		Reply(http.StatusOK).
		BodyString(res)
	return nil
}

func (c *Component) elasticsearchReturns(response *godog.DocString) error {
	c.ElasticSearchAPI.NewHandler().
		Post("/ons/_bulk").
		Reply(http.StatusOK).
		BodyString(response.Content)
	return nil
}

// thisModelIsSentToElasticSearch defines the assertion for elasticsearch POST /ons/_bulk
// Note that this assumes elasticsearchReturns has already been called before
// Otherwise an error will be returned
func (c *Component) thisModelIsSentToElasticSearch(es *godog.DocString) error {
	b := []byte(es.Content)
	// esa := &ElasticSearchAssertor{
	// 	Expected: b,
	// }
	// c.ElasticSearchAPI.NewHandler().
	// 	Post("/ons/_bulk").AssertCustom(esa).
	// 	Reply(http.StatusOK)

	esa, err := NewAssertor(b)
	if err != nil {
		return fmt.Errorf("failed to create elasticsearch assertor: %w", err)
	}

	ok := false
	for _, h := range c.ElasticSearchAPI.RequestHandlers {
		if h.Method == http.MethodPost && h.URL.Path == "/ons/_bulk" {
			h.AssertCustom(esa)
			ok = true
			break
		}
	}
	// time.Sleep(10 * time.Second)

	if err := waitForElasticSearchCall(WaitEventTimeout, esa); err != nil {
		return fmt.Errorf("error validating call to elasticsearch: %w", err)
	}

	if !ok {
		return errors.New("no response defined for elasticsearch POST /ons/_bulk, please define `elasticsearch returns the following response for bulk update`")
	}
	return nil
}

// notingSentToElasticSearch validates that no call is done for elasticsearch POST /ons/_bulk
func (c *Component) notingSentToElasticSearch() error {
	esa, err := NewAssertor(nil)
	if err != nil {
		return fmt.Errorf("failed to create elasticsearch assertor: %w", err)
	}

	c.ElasticSearchAPI.NewHandler().
		Post("/ons/_bulk").
		AssertCustom(esa)

	if err := waitNoElasticSearchCall(WaitEventTimeout, esa); err != nil {
		return fmt.Errorf("error validating that nothing was sent to elasticsearch: %w", err)
	}
	return nil
}

// thisSearchDataImportEventIsQueued produces a new SearchDataImport event with the contents defined by the input
func (c *Component) thisSearchDataImportEventIsQueued(table *godog.Table) error {
	assist := assistdog.NewDefault()
	assist.RegisterParser([]string{}, arrayParser)
	r, err := assist.CreateSlice(&models.SearchDataImportModel{}, table)
	if err != nil {
		return fmt.Errorf("failed to create slice from godog table: %w", err)
	}
	expected := r.([]*models.SearchDataImportModel)

	if err := c.KafkaConsumer.QueueMessage(schema.SearchDataImportEvent, expected[0]); err != nil {
		return fmt.Errorf("failed to queue event for testing: %w", err)
	}

	return nil
}

func waitForElasticSearchCall(timeWindow time.Duration, esa *ElasticSearchAssertor) error {
	delay := time.NewTimer(timeWindow)

	select {
	case <-delay.C:
		return errors.New("timeout while waiting for elasticsearch call")
	case r, ok := <-esa.called:
		if !delay.Stop() {
			<-delay.C
		}
		if !ok {
			return errors.New("elasticsearch assertor channel closed")
		}

		if diff := cmp.Diff(r.Queries, esa.Expected()); diff != "" {
			//nolint
			return fmt.Errorf("-got +expected)\n%s\n", diff)
		}
		return nil
	}
}

// waitNoElasticSearchCall waits until the timeWindow elapses.
// If during the time window elasticsearch is called then an error is returned
func waitNoElasticSearchCall(timeWindow time.Duration, esa *ElasticSearchAssertor) error {
	delay := time.NewTimer(timeWindow)

	select {
	case <-delay.C:
		return nil
	case r, ok := <-esa.called:
		if !delay.Stop() {
			<-delay.C
		}
		if !ok {
			return errors.New("elasticsearch assertor channel closed")
		}
		return fmt.Errorf("unexpected call to elasticsearch: %s %s", r.Req.Method, r.Req.URL.String())
	}
}

// we are passing the string array as [xxxx,yyyy,zzz]
// this is required to support array being used in kafka messages
func arrayParser(raw string) (interface{}, error) {
	// remove the starting and trailing brackets
	str := strings.Trim(raw, "[]")
	if str == "" {
		return []string{}, nil
	}

	strArray := strings.Split(str, ",")
	return strArray, nil
}
