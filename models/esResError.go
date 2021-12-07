package models

// esBulkItemResponseError holds an ES Error details
type ESBulkItemResponseError struct {
	ErrorType string `json:"type"`
	Reason    string `json:"reason"`
	IndexUUID string `json:"index_uuid"`
	Shard     string `json:"shard"`
	Index     string `json:"index"`
}
