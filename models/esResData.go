package models

// esBulkItemResponseData holds a response from ES
type ESBulkItemResponseData struct {
	Index  string                  `json:"_index"`
	ID     string                  `json:"_id"`
	Status int                     `json:"status"`
	Error  ESBulkItemResponseError `json:"error,omitempty"`
}
