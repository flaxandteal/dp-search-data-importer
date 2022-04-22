package models

// EsModel holds an individual content data
type EsModel struct {
	DataType        string   `json:"type"`
	URI             string   `json:"uri"`
	JobID           string   `json:"job_id"`
	SearchIndex     string   `json:"search_index"`
	CDID            string   `json:"cdid"`
	DatasetID       string   `json:"dataset_id"`
	Keywords        []string `json:"keywords"`
	MetaDescription string   `json:"meta_description"`
	ReleaseDate     string   `json:"release_date,omitempty"`
	Summary         string   `json:"summary"`
	Title           string   `json:"title"`
	Topics          []string `json:"topics"`
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
