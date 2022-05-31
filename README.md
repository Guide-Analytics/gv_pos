# Graph 2.0 Visualization of POS Taggings in Customer Reviews
- Graph 2.0 is a developed version of 1.0 (previously used on smaller dataset) 
- It's more focused on parse part-of-speech tags in reviews and extract out information
for each reviewer: what kind of verbs, adjectives, nouns are used in each review.
- As a collective whole, graph can analyze 
- Graph 2.0 is purely hard-coded algorithm based on Personalized Page Rank algorithm.
We want to understand what keywords are mostly talked about in relationship and association to all other 
words extracted in the reviews. 
- Basically what PageRank functionality does in simple terms is it provides the most
searched keywords in the reviews. Then, it exercise in extraction of other words that
has a certain probabilistic weight that are mentioned with association to the keywords. 
The lower the probability weight


### Future purposes:

- In theory, we can potentially catch fraudsters using this graph method, analyzing and detecting reviews
that are linked together based on adjectives, nouns, verbs used.

### Learn:
One node in the graph is designated as the source node. Personalize page rank is performed with respect to that
source node.

Personalized page rank is initialized by assigning all probability mass to the source node and none to the other
nodes. In contrast, ordinary page rank is initialized by giving all nodes the same probability mass.
Whenever personalized page rank makes a random jump, it jumps back to the source node. In contrast, ordinary page rank
may jump to any node.

In personalized page rank, all probability mass lost dandling nodes is put back into the source nodes.

### Advantages of using 2.0 versus 1.0:
- Graph is viable
- Fast
- Very unique data structure
- Easy to handle through Spark implementation (not easily implemented, but easily parsed through
without issues)

### Disadvantages of using 2.0 versus 1.0:
- Complicated to understand if fundamental graph theory is not developed
- Can be sometimes be hard to debug if certain nodes have issues
- Hard to implement hard-coded through Spark

### Challenges:
- Ordinary PageRank is harder than Personalized PageRank, but PPR can still be hard to understand, 
and therefore implementation is almost exhaustively long to do. 
- Most of the times, implementations were under the trial-and-error methodology.
- Source node weight update can be tricky if not coded properly. 
- Mathematical implementation and code type implementation in Spark RDD is extremely fragile. 
If one node is updated incorrectly  (i.e. string instead of int) or (i.e. incorrect weight value), 
entire Spark program crashes or all the weights for other nodes might get updated incorrectly or won't
update at all.

### Features:
- Inputs - raw GOOGLE_REVIEWS.csv
- Outputs - result of the top keywords with the most weight

### Installing Requirements:
Add configurations from 'requirements.txt'. If you're using an IDE, it will prompt you
to install the packages. Otherwise, simply run:
- _pip install <package_names>_


### Expected prerequisites:
You should know how to run Apache Spark on Python IDE. Make sure Apache Spark
and Pypsark package (Python) is running properly before executing the program 

### Resources:
- https://blog.sicara.com/fraud-detection-personalized-page-rank-networkx-15bd52ba2bf6
- http://lintool.github.io/MapReduceAlgorithms/MapReduce-book-final.pdf (Section 5.3. Page 102)

