Feature: Search data importer unhealthy


  Scenario: Not consuming events, because a elasticsearch is not healthy
    Given elasticsearch is unhealthy

    When the service starts
    And this search-data-import event is queued, to be consumed
    """
      {
        "URI":       "some_uri",
        "DataType":  "legacy",
        "DatasetID": "123"
      }
    """

    Then nothing is sent to elasticsearch
