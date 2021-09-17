package schema

import (
	"github.com/ONSdigital/go-ns/avro"
)

var publishedContentEvent = `{
  "type": "record",
  "name": "published-content",
  "fields": [
    {"name": "recipient_name", "type": "string", "default": ""}
  ]
}`

// PublishedContentEvent is the Avro schema for Published Content messages.
var PublishedContentEvent = &avro.Schema{
	Definition: publishedContentEvent,
}
