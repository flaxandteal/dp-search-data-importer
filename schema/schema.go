package schema

import (
	"github.com/ONSdigital/dp-kafka/v2/avro"
)

var publishedContentEvent = `{
  "type": "record",
  "name": "search-data-import",
  "fields": [
    {"name": "type", "type": "string", "default": ""},
    {"name": "job_id", "type": "string", "default": ""},
    {"name": "search_index", "type": "string", "default": ""},
    {"name": "cdid", "type": "string", "default": ""},
    {"name": "dataset_id", "type": "string", "default": ""},
    {"name": "keywords","type":["null",{"type":"array","items":"string"}]},
    {"name": "meta_description", "type": "string", "default": ""},
    {"name": "release_date", "type": "string", "default": ""},
    {"name": "summary", "type": "string", "default": ""},
    {"name": "title", "type": "string", "default": ""},
    {"name": "trace_id", "type": "string", "default": ""}
  ]
}`

// PublishedContentEvent is the Avro schema for Search Data Import messages.
var PublishedContentEvent = &avro.Schema{
	Definition: publishedContentEvent,
}
