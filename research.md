# Web Research

## https://en.wikipedia.org/wiki/Record_linkage

Record linkage is the process of deduplicating a data set from one or more sources

Data should be transformed into a uniform format so that later comparisons are easier e.g. transform all dates into MM/DD/YYYY

Binning allows for a more efficient comparison of similar records by reducing the number of candidate duplicates. This can efficiency can come at the cost of data integrity. Do we care more about data quality of speed of processing? 

There are rule based and probabilistic methods for matching records. Using a rule based methods would not be scaliable or flexible but would take advantage of domain knowledge. Probabilistic seems like the way to go.


## https://recordlinkage.readthedocs.io/en/latest/about.html

Looks like there is a python package that is build for record linkage. It supports binning and probabilistic matching. This seems like good for a prototyping project like this. 

This library requires that the data fit into a dataframe. The data given looks like it will need to be reformatted to fit. Will this reformatting cause loss of inforamtion?

## https://www.ncbi.nlm.nih.gov/pmc/articles/PMC1479910/
## http://iopscience.iop.org/article/10.1088/1742-6596/978/1/012118/pdf

# Thoughts

## Validation

It seems like validating data that has been deduplicated will be a challenge. Without knowing the number of actual unique entities in the dataset its really hard to measure how well our deduplication is doing. It may be worthwhile to build a few simple datasets that have know outputs so that I can unit tests my code.

## Speed

Does this code need to work at scale? Given the nature of this assignment I’m assuming that this project is more about how to do record linkage and not how to write code that will scale to millions of entries.



