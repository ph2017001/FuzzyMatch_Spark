# FuzzyMatch_Spark

**Goal**: The goal of this exercise is to develop a fuzzy record matching program that takes records from
“query” set and finds matching record in the “reference” set.

**Data**: In the data folder, the excel files contain two sheets:
- “reference” sheet  contains a list of restaurants, its address and cuisine (“reference” set)
- “query” sheet contains again a list of restaurants, its address and cuisine, plus two additional columns (“query” set)
  * “reference_id” column needs to be populated using the fuzzy record matching program
  * “score(optional)” field can show to what degree the record matches or the confidence of the match. It is optional though.


**Solution Approach**

To be able to produce an approximate match or matches from the reference set, it is important to have a distance metric that can provide a measure of similarity/dissimilarity. There are many such metrics:
- Levenshtein Distance
- Cosine Similarity
- Jaccard Distance
- Hamming Distance, etc.

If the dataset is small or enough computing power is available, it is possible to do all possible pairwise calculation of distances and find the nearest match.
However, practically this approach is not feasible. So, the solution should find a way:

- To partially guess the distance between two vectors.
- Perform calculations in parallel/distributed environment.
- Provide faster search over a partitioned dataset. 
