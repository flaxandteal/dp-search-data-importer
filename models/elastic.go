package models

// EsModel holds an individual content data
type EsModel struct {
	DataType        string              `json:"type"`
	URI             string              `json:"uri"`
	JobID           string              `json:"job_id"`
	SearchIndex     string              `json:"search_index"`
	CDID            string              `json:"cdid"`
	DatasetID       string              `json:"dataset_id"`
	Edition         string              `json:"edition"`
	Keywords        []string            `json:"keywords"`
	MetaDescription string              `json:"meta_description"`
	ReleaseDate     string              `json:"release_date,omitempty"`
	Summary         string              `json:"summary"`
	Title           string              `json:"title"`
	Topics          []string            `json:"topics"`
	DateChanges     []ReleaseDateChange `json:"date_changes,omitempty"`
	Cancelled       bool                `json:"cancelled"`
	Finalised       bool                `json:"finalised"`
	ProvisionalDate string              `json:"provisional_date,omitempty"`
	Published       bool                `json:"published"`
	Language        string              `json:"language,omitempty"`
	Survey          string              `json:"survey,omitempty"`
	CanonicalTopic  string              `json:"canonical_topic"`
	PopulationType  *EsPopulationType   `json:"population_type"`
	Dimensions      []EsDimension       `json:"dimensions"`
}

// EsBulkResponse holds a response from ES
type EsBulkResponse struct {
	Took   int                  `json:"took"`
	Errors bool                 `json:"errors"`
	Items  []EsBulkItemResponse `json:"items"`
}

type EsBulkItemResponse map[string]EsBulkItemResponseData

// EsBulkItemResponseData holds a response from ES for each item
type EsBulkItemResponseData struct {
	Index  string                  `json:"_index"`
	ID     string                  `json:"_id"`
	Status int                     `json:"status"`
	Error  EsBulkItemResponseError `json:"error,omitempty"`
}

// EsBulkItemResponseError holds an ES Error details
type EsBulkItemResponseError struct {
	ErrorType string `json:"type"`
	Reason    string `json:"reason"`
	IndexUUID string `json:"index_uuid"`
	Shard     string `json:"shard"`
	Index     string `json:"index"`
}

// ReleaseDateChange represent a date change of a release
type ReleaseDateChange struct {
	ChangeNotice string `json:"change_notice"`
	Date         string `json:"previous_date"`
}

// EsPopulationType represents the population type information in an elastic-search json
type EsPopulationType struct {
	Name   string `json:"name"`
	Label  string `json:"label"`
	AggKey string `json:"agg_key"`
}

// EsDimension represents a dimension in an elastic-search json
type EsDimension struct {
	Name     string `json:"name"`
	RawLabel string `json:"raw_label"`
	Label    string `json:"label"`
	AggKey   string `json:"agg_key"`
}
