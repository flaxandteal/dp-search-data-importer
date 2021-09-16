package event

// TODO: remove hello called example model
// HelloCalled provides an avro structure for a Hello Called event
type PublishedContent struct {
	RecipientName string `avro:"recipient_name"`
}
