package steps

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/ONSdigital/dp-search-data-importer/models"
	"github.com/ONSdigital/dp-search-data-importer/schema"
	"github.com/cucumber/godog"
	"github.com/google/go-cmp/cmp"
)

func (c *Component) RegisterSteps(ctx *godog.ScenarioContext) {
	ctx.Step(`^the service starts`, c.theServiceStarts)
	ctx.Step(`^elasticsearch is healthy`, c.elasticSearchIsHealthy)
	ctx.Step(`^elasticsearch is unhealthy`, c.elasticSearchIsUnhealthy)
	ctx.Step(`^elasticsearch returns the following response for bulk update$`, c.elasticsearchReturns)
	ctx.Step(`^this search-data-import event is queued, to be consumed$`, c.thisSearchDataImportEventIsQueued)
	ctx.Step(`^nothing is sent to elasticsearch`, c.notingSentToElasticsearch)
	ctx.Step(`^this model is sent to elasticsearch$`, c.thisModelIsSentToElasticsearch)
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
	c.ElasticsearchAPI.NewHandler().
		Get("/_cluster/health").
		Reply(http.StatusOK).
		BodyString(res)
	return nil
}

// elasticSearchIsUnhealthy generates a mocked unhealthy response for elasticsearch healthecheck
func (c *Component) elasticSearchIsUnhealthy() error {
	const res = `{"cluster_name": "docker-cluster", "status": "red"}`
	c.ElasticsearchAPI.NewHandler().
		Get("/_cluster/health").
		Reply(http.StatusOK).
		BodyString(res)
	return nil
}

func (c *Component) elasticsearchReturns(response *godog.DocString) error {
	c.ElasticsearchAPI.NewHandler().
		Post("/ons/_bulk").
		Reply(http.StatusOK).
		BodyString(response.Content)
	return nil
}

// thisModelIsSentToElasticsearch defines the assertion for elasticsearch POST /ons/_bulk
// Note that this assumes elasticsearchReturns has already been called before
// Otherwise an error will be returned
func (c *Component) thisModelIsSentToElasticsearch(es *godog.DocString) error {
	b := []byte(es.Content)

	esa, err := NewAssertor(b)
	if err != nil {
		return fmt.Errorf("failed to create elasticsearch assertor: %w", err)
	}

	ok := false
	for _, h := range c.ElasticsearchAPI.RequestHandlers {
		if h.Method == http.MethodPost && h.URL.Path == "/ons/_bulk" {
			h.AssertCustom(esa)
			ok = true
			break
		}
	}

	if err := waitForElasticsearchCall(WaitEventTimeout, esa); err != nil {
		return fmt.Errorf("error validating call to elasticsearch: %w", err)
	}

	if !ok {
		return errors.New("no response defined for elasticsearch POST /ons/_bulk, please define `elasticsearch returns the following response for bulk update`")
	}
	return nil
}

// notingSentToElasticsearch validates that no call is done for elasticsearch POST /ons/_bulk
func (c *Component) notingSentToElasticsearch() error {
	esa, err := NewAssertor(nil)
	if err != nil {
		return fmt.Errorf("failed to create elasticsearch assertor: %w", err)
	}

	c.ElasticsearchAPI.NewHandler().
		Post("/ons/_bulk").
		AssertCustom(esa)

	if err := waitNoElasticsearchCall(WaitEventTimeout, esa); err != nil {
		return fmt.Errorf("error validating that nothing was sent to elasticsearch: %w", err)
	}
	return nil
}

// thisSearchDataImportEventIsQueued produces a new SearchDataImport event with the contents defined by the input
func (c *Component) thisSearchDataImportEventIsQueued(eventDocString *godog.DocString) error {
	event := &models.SearchDataImport{}
	if err := json.Unmarshal([]byte(eventDocString.Content), event); err != nil {
		return fmt.Errorf("failed to unmarshal docstring to search data import event: %w", err)
	}

	if err := c.KafkaConsumer.QueueMessage(schema.SearchDataImportEvent, event); err != nil {
		return fmt.Errorf("failed to queue event for testing: %w", err)
	}
	return nil
}

// waitForElasticsearchCall waits for a call to the provided Elasticsearch assessor, and it validates it against the expected body
// If the validation fails or the timeout expires, an error is returned
func waitForElasticsearchCall(timeWindow time.Duration, esa *ElasticsearchAssertor) error {
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

// waitNoElasticsearchCall waits until the timeWindow elapses.
// If during the time window elasticsearch is called then an error is returned
func waitNoElasticsearchCall(timeWindow time.Duration, esa *ElasticsearchAssertor) error {
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
