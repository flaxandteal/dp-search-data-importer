package models

// EsModel holds an individual content data
type EsModel struct {
	DataType        string   `json:"type"`
	JobID           string   `json:"job_id"`
	SearchIndex     string   `json:"search_index"`
	CDID            string   `json:"cdid"`
	DatasetID       string   `json:"dataset_id"`
	Keywords        []string `json:"keywords"`
	MetaDescription string   `json:"meta_description"`
	ReleaseDate     string   `json:"release_date,omitempty"`
	Summary         string   `json:"summary"`
	Title           string   `json:"title"`
}
