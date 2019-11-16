import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.functions._
case class edges(Source: String, Target: String, Weight: Int)
import spark.implicits._

// COMMAND ----------

val df = spark.read.textFile("/FileStore/tables/bitcoinotc.csv") 
  .map(_.split(","))
  .map(columns => edges(columns(0), columns(1), columns(2).toInt)).toDF()

display(df)

// COMMAND ----------

// eliminate duplicate rows
val data_dup = df.select($"Source", $"Target", $"Weight").dropDuplicates()
display(data_dup)

// COMMAND ----------

// filter nodes by edge weight >= supplied threshold in assignment instructions
val data = data_dup.filter(col("Weight") >= 5)
display(data)


// COMMAND ----------

val sourceIDs = data.select(df("Source")).distinct
val sources = data.select(data("Source"), data("Weight"))
display(sources)
val targetIDs = data.select(data("Target")).distinct
val targets = data.select(data("Target"), data("Weight"))
display(targets)
val finalColumns = Seq("v", "d", "c")

// COMMAND ----------

// Identifying the node with highest weighted-in-degree. If two or more nodes have the same weighted-in-degree, the one with the lowest node id is reported
val indegree = targets.groupBy("Target").sum("Weight")
val inNewColumns = Seq("Node", "Weighted-In-Degree")
val indegreeS = indegree.toDF(inNewColumns:_*).withColumn("c",org.apache.spark.sql.functions.lit("i"))
val indegreeMax = indegreeS.orderBy(desc("Weighted-In-Degree"), asc("Node"))
display(indegreeMax)
val inMax = indegreeMax.toDF(finalColumns:_*).limit(1)
display(inMax)

// COMMAND ----------

// Identifying the node with highest weighted-out-degree. If two or more nodes have the same weighted-out-degree, the one with the lowest node id is reported
val outdegree = sources.groupBy("Source").sum("Weight")
val outNewColumns = Seq("Node", "Weighted-Out-Degree")
val outdegreeS = outdegree.toDF(outNewColumns:_*).withColumn("c",org.apache.spark.sql.functions.lit("o"))
val outdegreeMax = outdegreeS.orderBy(desc("Weighted-Out-Degree"), asc("Node"))
display(outdegreeMax)
val outMax = outdegreeMax.toDF(finalColumns:_*).limit(1) 
display(outMax)

// COMMAND ----------

// Identifying the node with highest weighted-total degree. If two or more nodes have the same weighted-total-degree, the one with the lowest node id is reported

val out_in_total = indegree.union(outdegree)
val totaldegreeS = out_in_total.groupBy("Target").sum("sum(Weight)")

val totaldegreeMax = totaldegreeS.orderBy(desc("sum(sum(Weight))"), asc("Target")).withColumn("c",org.apache.spark.sql.functions.lit("t"))
display(totaldegreeMax)

val totalMax = totaldegreeMax.toDF(finalColumns:_*).limit(1)
display(totalMax)


// COMMAND ----------

/*
Dataframe to store the results
Schema: 3 columns, named: 'v', 'd', 'c' where:
'v' : vertex id
'd' : degree calculation (an integer value.  one row with highest weighted-in-degree, a row w/ highest weighted-out-degree, a row w/ highest weighted-total-degree )
'c' : category of degree, containing one of three string values:
                                                'i' : weighted-in-degree
                                                'o' : weighted-out-degree                                                
                                                't' : weighted-total-degree


*/
val finalDF = (inMax.union(outMax)).union(totalMax)
display(finalDF)
