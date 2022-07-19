package schema

import (
	"github.com/ONSdigital/dp-kafka/v2/avro"
)

var searchDataImportEvent = `{
  "type": "record",
  "name": "search-data-import",
  "fields": [
    {"name": "uid", "type": "string", "default": ""},
    {"name": "uri", "type": "string", "default": ""},
    {"name": "type", "type": "string", "default": ""},
    {"name": "job_id", "type": "string", "default": ""},
    {"name": "search_index", "type": "string", "default": ""},
    {"name": "cdid", "type": "string", "default": ""},
    {"name": "dataset_id", "type": "string", "default": ""},
    {"name": "edition", "type": "string", "default": ""},
    {"name": "keywords", "type": {"type":"array","items":"string"}},
    {"name": "meta_description", "type": "string", "default": ""},
    {"name": "release_date", "type": "string", "default": ""},
    {"name": "summary", "type": "string", "default": ""},
    {"name": "title", "type": "string", "default": ""},
    {"name": "topics", "type": {"type":"array","items":"string"}},
    {"name": "trace_id", "type": "string", "default": ""},
    {"name": "cancelled", "type": "boolean", "default": false},
    {"name": "finalised", "type": "boolean", "default": false},
    {"name": "published", "type": "boolean", "default": false},
    {"name": "language", "type": "string", "default": ""},
    {"name": "survey", "type": "string", "default": ""},
    {"name": "canonical_topic", "type": "string", "default": ""},
    {"name": "date_changes", "type": {"type":"array","items":{
     "name": "ReleaseDateDetails",
     "type" : "record",
     "fields" : [
      {"name": "change_notice", "type": "string", "default": ""},
      {"name": "previous_date", "type": "string", "default": ""}
    ]}}},
    {"name": "provisional_date", "type": "string", "default": ""}
  ]
}`

// SearchDataImportEvent is the Avro schema for Search Data Import messages.
var SearchDataImportEvent = &avro.Schema{
	Definition: searchDataImportEvent,
}
