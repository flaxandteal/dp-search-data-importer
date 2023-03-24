package models

// SearchDataImport provides an avro structure for a SearchDataImport Called event
type SearchDataImport struct {
	UID             string               `avro:"uid"`
	URI             string               `avro:"uri"`
	Edition         string               `avro:"edition"`
	DataType        string               `avro:"data_type"`
	JobID           string               `avro:"job_id"`
	SearchIndex     string               `avro:"search_index"`
	CDID            string               `avro:"cdid"`
	DatasetID       string               `avro:"dataset_id"`
	Keywords        []string             `avro:"keywords"`
	MetaDescription string               `avro:"meta_description"`
	ReleaseDate     string               `avro:"release_date"`
	Summary         string               `avro:"summary"`
	Title           string               `avro:"title"`
	Topics          []string             `avro:"topics"`
	TraceID         string               `avro:"trace_id"`
	DateChanges     []ReleaseDateDetails `avro:"date_changes"`
	Cancelled       bool                 `avro:"cancelled"`
	Finalised       bool                 `avro:"finalised"`
	ProvisionalDate string               `avro:"provisional_date"`
	CanonicalTopic  string               `avro:"canonical_topic"`
	Published       bool                 `avro:"published"`
	Language        string               `avro:"language"`
	Survey          string               `avro:"survey"`
	PopulationType  PopulationType       `avro:"population_type"`
	Dimensions      []Dimension          `avro:"dimensions"`
}

// ReleaseDateDetails represents a change of release date
type ReleaseDateDetails struct {
	ChangeNotice string `avro:"change_notice"`
	Date         string `avro:"previous_date"`
}

// Dimension represents the required information for each dataset dimension: name (unique ID) and label
type Dimension struct {
	Name     string `avro:"name"`
	RawLabel string `avro:"raw_label"`
	Label    string `avro:"label"`
	AggKey   string `avro:"agg_key"`
}

// PopulationType represents the population type name (unique ID) and label
type PopulationType struct {
	Name   string `avro:"name"`
	Label  string `avro:"label"`
	AggKey string `avro:"agg_key"`
}
