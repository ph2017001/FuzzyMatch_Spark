# FuzzyMatch_Spark

**Goal**: The goal of this exercise is to develop a fuzzy record matching program that takes records from
“query” set and finds matching record in the “reference” set.

**Data**: In the data folder, the excel files contain two sheets:
- “reference” sheet  contains a list of restaurants, its address and cuisine (“reference” set)
- “query” sheet contains again a list of restaurants, its address and cuisine, plus two additional columns (“query” set)
  * “reference_id” column needs to be populated using the fuzzy record matching program
  * “score(optional)” field can show to what degree the record matches or the confidence of the match. It is optional though.
