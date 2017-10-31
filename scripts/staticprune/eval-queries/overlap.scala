val fBase = "/mnt/work2/results/cw09b.baseline.OR.postings.txt"
val tBase = "/mnt/ssd/qs-xdoc/cw09b-text.terms"
val dBase = "/mnt/ssd/qs-xdoc/cw09b.titles"

val fName = "/mnt/work2/results/all.top1k-01.OR.postings.txt"
val tName = "/mnt/work2/pruned/all.top1k-20-0.terms"
val dName = "/mnt/work2/pruned/all.top1k-20.titles"

val fPostings = "/mnt/work2/pruned/all-top1k-20.features"

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

sc.setLogLevel("ERROR")

// add unique index column (row number)
def addIndex(df: org.apache.spark.sql.DataFrame) = spark.sqlContext.createDataFrame(
  // Add index
  df.rdd.zipWithIndex.map{case (r, i) => Row.fromSeq(r.toSeq :+ i)},
  // Create schema
  StructType(df.schema.fields :+ StructField("_index", LongType, false))
)

// baseline terms, docs
val bTerms = addIndex(spark.read.text(tBase).toDF("term")).withColumn("termID",$"_index").select("term","termID")
val bDocs = addIndex(spark.read.text(dBase).toDF("doc")).withColumn("docID",$"_index").select("doc","docID")

// PRUNED terms, docs
val pTerms = addIndex(spark.read.text(tName).toDF("term")).withColumn("lTermID",$"_index").select("term","lTermID")
val pDocs = addIndex(spark.read.text(dName).toDF("doc")).withColumn("lDocID",$"_index").select("doc","lDocID")

// map from local ID to global ID
val termMap = pTerms.join(bTerms, Seq("term"), "inner")
termMap.persist

val docMap = pDocs.join(bDocs, Seq("doc"), "inner")
docMap.persist

// get list of pruned postings, with global IDs
val dd = spark.read.csv(fPostings).toDF("lTermID","lDocID")
  .withColumn("lTermID",$"lTermID".cast("Int"))
  .withColumn("lDocID",$"lDocID".cast("Int"))
  .join(termMap, Seq("lTermID"), "inner")
  .join(docMap, Seq("lDocID"), "inner")
  .select("termID","docID")

dd.persist

//  BASELINE: global term IDs, global docIDs
val baseline = spark.read.option("header","false").csv(fBase)
  .toDF("query","docID","termID","rank")
//  .withColumn("query",$"query".cast("int"))
  .withColumn("docID",$"docID".cast("int"))
  .withColumn("termID",$"termID".cast("int"))
  .withColumn("rank",$"rank".cast("int"))
  .filter($"termID".isNotNull)
  .select("docID","termID","rank")

baseline.persist

// postings hit in baseline @1k
val b1k = baseline.select("docID","termID").distinct
b1k.persist

// postings hit in baseline @10
val b10 = baseline.filter($"rank" <= 10).select("docID","termID").distinct
b10.persist

// overlap in pruned index
val o1k = b1k.join(dd, Seq("termID","docID"), "inner")
val o10 = b10.join(dd, Seq("termID", "docID"), "inner")

println("baseline @1k = " + b1k.count)
println("baseline @10 = " + b10.count)
println("overlap @1k " + o1k.count)
println("overlap @10 " + o10.count)

// local termIDs, global docIDs
// val d = spark.read.option("header","false").csv(fName)
//   .toDF("query","docID","lTermID","rank")
// //  .withColumn("query",$"query".cast("int"))
//   .withColumn("docID",$"docID".cast("int"))
//   .withColumn("lTermID",$"lTermID".cast("int"))
//   .withColumn("rank",$"rank".cast("int"))
//   .filter($"lTermID".isNotNull)
//   .select("docID","lTermID","lRank")
//   .join(termMap, Seq("lTermID"), "inner")


