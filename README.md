-------------------------------------------------------
# Analyzing a Large Graph with Spark/Scala on Databricks
-------------------------------------------------------
This scala code was completed as part of an assignment for my Data and Visual Analytics course at The Georgia Insitute of Technology. 

### Introduction
The [Bitcoin OTC]("https://snap.stanford.edu/data/soc-sign-bitcoin-otc.html" target="_blank") Trust Weighted Signed Network maintains a record of users' reputation to prevent transactions with fraudulent and risky users. This graph is a who-trusts-whom network of people who trade using the platform, and is comprised of almost 6,000 nodes and over 35,000 edges. I analyzed this Bitcoin OTC network using [DataFrame API](https://spark.apache.org/docs/2.3.1/api/scala/index.html#org.apache.spark.sql.Dataset) in Spark and Scala on the Databricks platform.

Objectives:
1. Eliminate any duplicate rows.
2. Filter the graph such that only nodes containing an edge weight >= 5 are preserved.  
3. Analyze the graph to find the nodes with the highest weighted-in-degree, weighted-out-degree, and weighted-total-degree using DataFrame operations.
4. Download a new DataFrame to output.csv containing my analysis (schema provided below).

|v: vertex id |d : weighted-degree value |c: category of weighted-degree |
|:----------|:-------------|:-------------|
| | |i: weighted-in-degree|
| | |o: weighted-out-degree|
| | |t : weighted-total-degree|

### Prerequisites
This code runs on the Databricks platform using a community account. Before running this code, create an [account](https://databricks.com/try-databricks) on Databricks. Only a community account is necessary. 

### Run
To run the code on the bitcoinotc.csv edge file, 
1. Import the bitcoinotc.csv data into your data space on Databricks.
2. Import the BitcoinOTC.dbc notebook to your workspace on Databricks. 
3. Create a cluster.
4. Attach the cluster to the imported notebook.
5. Run the code cells.
