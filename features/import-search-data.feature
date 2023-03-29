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
          "Key": "pop-label",
          "AggKey": "pop-label###Pop Label",
          "Name":  "popName",
          "Label": "Pop Label"
        },
        "Dimensions": [
          {
            "Key": "label-1",
            "AggKey": "label-1###Label 1",
            "Name":     "dim1,dim2",
            "Label":    "Label 1",
            "RawLabel": "Label 1 (10 categories),Label 1 (20 categories)"
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
            "key": "pop-label",
            "agg_key": "pop-label###Pop Label",
            "name":  "popName",
            "label": "Pop Label"
          },
          "dimensions": [
 		        {
              "key": "label-1",
              "agg_key": "label-1###Label 1",
 		          "name":      "dim1,dim2",
 		          "label":     "Label 1",
              "raw_label": "Label 1 (10 categories),Label 1 (20 categories)"
 		        }
 		      ]
        },
        "doc_as_upsert":true
      }
    """
