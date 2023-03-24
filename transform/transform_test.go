package transform_test

import (
	"testing"

	"github.com/ONSdigital/dp-search-data-importer/models"
	"github.com/ONSdigital/dp-search-data-importer/transform"
	. "github.com/smartystreets/goconvey/convey"
)

func TestTransformEventModelToEsModel(t *testing.T) {
	Convey("Given a fully populated search data import model", t, func() {
		in := &models.SearchDataImport{
			DataType:        "test-type",
			Edition:         "test-edition",
			URI:             "test-uri",
			JobID:           "test-job-id",
			SearchIndex:     "test-search-index",
			CDID:            "test-cdid",
			DatasetID:       "test-dataset-id",
			Keywords:        []string{"k1", "k2"},
			MetaDescription: "test-meta-description",
			ReleaseDate:     "test-release-date",
			Summary:         "test-summary",
			Title:           "test-title",
			Topics:          []string{"t1", "t2"},
			Cancelled:       false,
			Finalised:       true,
			ProvisionalDate: "test-provisional-date",
			Published:       true,
			Survey:          "test-survey",
			Language:        "test-language",
			CanonicalTopic:  "test-canonical-topic",
			DateChanges: []models.ReleaseDateDetails{
				{
					ChangeNotice: "test-notice",
					Date:         "test-date",
				},
			},
			PopulationType: models.PopulationType{
				Name:   "test-name",
				Label:  "test-label",
				AggKey: "test-name###test-label",
			},
			Dimensions: []models.Dimension{
				{
					Name:     "dim1",
					Label:    "label1",
					RawLabel: "raw-label-1",
					AggKey:   "dim1###label1",
				},
			},
		}

		Convey("Then it is correctly transformed to an elasticsearch model with the same values", func() {
			tr := transform.NewTransformer()
			out := tr.TransformEventModelToEsModel(in)
			So(out, ShouldResemble, &models.EsModel{
				DataType:        "test-type",
				Edition:         "test-edition",
				URI:             "test-uri",
				JobID:           "test-job-id",
				SearchIndex:     "test-search-index",
				CDID:            "test-cdid",
				DatasetID:       "test-dataset-id",
				Keywords:        []string{"k1", "k2"},
				MetaDescription: "test-meta-description",
				ReleaseDate:     "test-release-date",
				Summary:         "test-summary",
				Title:           "test-title",
				Topics:          []string{"t1", "t2"},
				Cancelled:       false,
				Finalised:       true,
				ProvisionalDate: "test-provisional-date",
				Published:       true,
				Survey:          "test-survey",
				Language:        "test-language",
				CanonicalTopic:  "test-canonical-topic",
				DateChanges: []models.ReleaseDateChange{
					{
						ChangeNotice: "test-notice",
						Date:         "test-date",
					},
				},
				PopulationType: &models.EsPopulationType{
					Name:   "test-name",
					Label:  "test-label",
					AggKey: "test-name###test-label",
				},
				Dimensions: []models.EsDimension{
					{
						Name:     "dim1",
						Label:    "label1",
						RawLabel: "raw-label-1",
						AggKey:   "dim1###label1",
					},
				},
			})
		})
	})
}
