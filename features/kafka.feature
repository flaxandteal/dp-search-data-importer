Feature: PublishedContent

  Scenario: Posting and checking a response
    When these published contents are consumed:
            | RecipientName | 
            | Tim           |
    Then I should receive a published-content response