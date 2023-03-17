Feature: Search data imported to elasticsearch


  Scenario: When a search-data-import event is received, the corresponding model is stored in elasticsearch
    Given elasticsearch is healthy
    And elasticsearch returns the following response for bulk update
    """
      {
        "took": 13,
        "errors": false,
        "items": []
      }
    """

    When the service starts
    And this search-data-import event is queued, to be consumed
    """
      {
        "UID":       "cphi01-timeseries",
        "URI":       "some_uri",
        "DatasetID": "cphi01",
        "Edition":   "timeseries",
        "DataType":  "cantabular",
        "PopulationType": {
          "Name":  "popName",
          "Label": "popLabel"
        },
        "Dimensions": [
          {
            "Name":     "dim1",
            "Label":    "label1",
            "RawLabel": "rawLabel1"
          }
        ]
      }
    """

    Then this model is sent to elasticsearch
    """
      {
        "update":{
          "_id":"cphi01-timeseries"
        }
      }
      {
        "doc":{
          "type":"cantabular",
          "uri":"some_uri",
          "job_id":"",
          "search_index":"",
          "cdid":"",
          "dataset_id":"cphi01",
          "edition":"timeseries",
          "keywords":[],
          "meta_description":"",
          "summary":"",
          "title":"",
          "topics":[],
          "cancelled":false,
          "finalised":false,
          "published":false,
          "canonical_topic":"",
          "population_type": {
            "name":  "popName",
            "label": "popLabel"
          },
          "dimensions": [
 		        {
 		          "name":      "dim1",
 		          "raw_label": "rawLabel1",
 		          "label":     "label1"
 		        }
 		      ]
        },
        "doc_as_upsert":true
      }
    """
