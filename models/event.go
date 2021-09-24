package models

// PublishedContentExtracted provides an avro structure for a PublishedContentExtracted Called event
type PublishedContentExtracted struct {
	DataType        string `avro:"type"`
	JobID           string `avro:"job_id"`
	SearchIndex     string `avro:"search_index"`
	CDID            string `avro:"cdid"`
	DatasetID       string `avro:"dataset_id"`
	Keywords        string `avro:"keywords"`
	MetaDescription string `avro:"meta_description"`
	ReleaseDate     string `avro:"release_date"`
	Summary         string `avro:"summary"`
	Title           string `avro:"title"`
	TraceID         string `avro:"trace_id"`
}