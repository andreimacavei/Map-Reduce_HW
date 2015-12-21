# Indexing files using Map-Reduce paradigm

Given a number N of documents, determine the pairs of documents with a similarity grade higher than X.
Distribute the workload using Map-Reduce paradigm.

The entire process is composed of 3 stages:

1. Map Stage: starting from a list of documents, create threads that will each represent a map task wich will index part of a document (for complete homework rules see here https://docs.google.com/document/d/12v4ATutR1w6rRqSHdl2rZP_AOS1BzKqCG4R_NiTeflE/edit)
2. Reduce Stage: having the results from Map Stage, create Reduce Tasks (on a separate thread pool) that will combine the hash tables with word occurences into a single hash
3. Compare Stage: again, on a different thread pool, compute the necessaries calculation to determine the pairs of documents with a similarity grade higher than a given X.


