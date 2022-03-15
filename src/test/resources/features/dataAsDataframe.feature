Feature: Read data as DataTable and compare

@dataframeFeature
  Scenario: Read data as DataTable and compare

  Given I have the following books in the store
   |id:int:false     | title                                | author      |
   | 1     | The Devil in the White City          | Erik Larson |
   | 2     | The Lion, the Witch and the Wardrobe | C.S. Lewis  |
   | 3     | In the Garden of Beasts              | Erik Larson |
  When Available book in store
    | id | availability:int:false |
    | 1  | 10                     |
    | 2  | 20                     |
    | 3  | 15                     |