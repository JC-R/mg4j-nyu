// spark-shell --master local[*] --packages com.databricks:spark-csv_2.10:1.3.0 driver-memory 40g --name prune-features --conf spark.kryoserializer.buffer.max=256m
sc.setLogLevel("ERROR")
val work_dir = "/home/juan/save/data/"
val toInt    = udf[Int, String]( _.toInt)
val toDouble = udf[Double, String]( _.toDouble)

// 1. read fake query posthits 
// ---------------------------
//
// *** NOTE **** there are 2 versions of the posthit aggregator; one created the key as (doc,term), the other as (term,doc).
//  make sure to order this correctly here
val ph_features = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").load(work_dir+"gov2_60M_OR.ph.bins-0").
withColumn("docID",toInt($"C0")).
withColumn("termID",toInt($"C1")). 
withColumn("b1",toInt($"C2")).
withColumn("b2",toInt($"C3")).
withColumn("b3",toInt($"C4")).
withColumn("b4",toInt($"C5")).
withColumn("b5",toInt($"C6")).
withColumn("b6",toInt($"C7")).
withColumn("b7",toInt($"C8")).
withColumn("b8",toInt($"C9")).
withColumn("b9",toInt($"C10")).
withColumn("b10",toInt($"C11")).
withColumn("b20",toInt($"C12")).
withColumn("b40",toInt($"C13")).
withColumn("b80",toInt($"C14")).
withColumn("b160",toInt($"C15")).
withColumn("b320",toInt($"C16")).
withColumn("b640",toInt($"C17")).
withColumn("b1280",toInt($"C18")).
withColumn("top10",$"b1"+$"b2"+$"b3"+$"b4"+$"b5"+$"b6"+$"b7"+$"b8"+$"b9"+$"b10").
withColumn("top20",$"top10"+$"b20").
withColumn("top40",$"top10"+$"b40").
withColumn("top80",$"top10"+$"b80").
withColumn("top160",$"top10"+$"b160").
withColumn("top1k",$"top160"+$"b320"+$"b640"+$"b1280").
select("termID","docID","b1","b2","b3","b4","b5","b6","b7","b8","b9","b10",
"b20","b40","b80","b160","b320","b640","b1280",
"top10","top20","top40","top80","top160","top1k")

// 2. read gov2 index features
val index_features = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").load(work_dir+"gov2-postings.features").
withColumn("termID",toInt($"C0")).
withColumn("docID",toInt($"C1")).
withColumn("term_freq",toInt($"C2")).
withColumn("doc_term_freq",toInt($"C3")).
withColumn("bm25",toDouble($"C4")).
select("termID","docID","term_freq","doc_term_freq","bm25")

// 3. read document hits bins
val dh_features = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").load(work_dir+"gov2.features.csv").
withColumn("docID",toInt($"C0")).
withColumn("len",toInt($"C24")).
withColumn("terms",toInt($"C25")).
withColumn("xdoc",toDouble($"C23")).
withColumn("doc_ratio", toDouble($"len")/ toDouble($"terms")).
withColumn("xdoc_norm1", toDouble($"xdoc")/ toDouble($"len")).
withColumn("xdoc_norm2", toDouble($"xdoc")/ toDouble($"terms")).
withColumn("b1",toInt($"C7")).
withColumn("b2",toInt($"C8")).
withColumn("b3",toInt($"C9")).
withColumn("b4",toInt($"C10")).
withColumn("b5",toInt($"C11")).
withColumn("b6",toInt($"C12")).
withColumn("b7",toInt($"C13")).
withColumn("b8",toInt($"C14")).
withColumn("b9",toInt($"C15")).
withColumn("b10",toInt($"C16")).
withColumn("b20",toInt($"C17")).
withColumn("b40",toInt($"C18")).
withColumn("b80",toInt($"C19")).
withColumn("b160",toInt($"C20")).
withColumn("b320",toInt($"C21")).
withColumn("b640",toInt($"C22")).
withColumn("b1280",toInt($"C23")).
withColumn("top10",$"b1"+$"b2"+$"b3"+$"b4"+$"b5"+$"b6"+$"b7"+$"b8"+$"b9"+$"b10").
withColumn("top20",$"top10"+$"b20").
withColumn("top40",$"top10"+$"b40").
withColumn("top80",$"top10"+$"b80").
withColumn("top160",$"top10"+$"b160").
withColumn("top1k",$"top160"+$"b320"+$"b640"+$"b1280").
select("docID","len","terms","xdoc","doc_ratio","xdoc_norm1","xdoc_norm2",
"b1","b2","b3","b4","b5","b6","b7","b8","b9","b10","b20","b40","b80","b160","b320","b640","b1280",
"top10","top20","top40","top80","top160","top1k")

// 4. read training queries posthits
val train_features = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").load(work_dir+"gov2.100K_OR.ph.bins.csv").
toDF("termID","docID","b1","b2","b3","b4","b5","b6","b7","b8","b9","b10","b20","b40","b80","b160","b320","b640","b1280","top10","top1k").
select("termID","docID","top10","top1k")

// 5. save packed
ph_features.write.parquet(work_dir+"gov2_ph_OR_60M_bins.parquet")
dh_features.write.parquet(work_dir+"gov2_dh_features.parquet")
train_features.write.parquet(work_dir+"gov2_ph_100K_OR_bins.parquet")
index_features.write.parquet(work_dir+"gov2_index_features.parquet")

// 6. join index-features + fake-query posthit features + fake query dochits features + training queries posthits features
// -----------------------------------------------------------------------------------------------------------------------
sc.setLogLevel("ERROR")
val work_dir = "/home/juan/save/"
val toInt    = udf[Int, String]( _.toInt)
val toDouble = udf[Double, String]( _.toDouble)

// select a random  sample 
val index_features = sqlContext.read.parquet(work_dir+"gov2_index_features.parquet").sample(false,0.05)
val dh = sqlContext.read.parquet(work_dir+"gov2_dh_features.parquet").
toDF("d_docID","d_len","d_terms","d_xdoc","d_doc_ratio","d_xdoc_norm1","d_xdoc_norm2",
"d_b1","d_b2","d_b3","d_b4","d_b5","d_b6","d_b7","d_b8","d_b9","d_b10","d_b20","d_b40","d_b80","d_b160","d_b320","d_b640","d_b1280",
"d_top10","d_top20","d_top40","d_top80","d_top160","d_top1k")

val df1 = index_features.join(dh, $"d_docID" === $"docID", "inner")
df1.cache()

val ph = sqlContext.read.parquet(work_dir+"gov2_ph_OR_60M_bins.parquet").
toDF("p_termID","p_docID","p_b1","p_b2","p_b3","p_b4","p_b5","p_b6","p_b7","p_b8","p_b9","p_b10",
"p_b20","p_b40","p_b80","p_b160","p_b320","p_b640","p_b1280",
"p_top10","p_top20","p_top40","p_top80","p_top160","p_top1k")

val df2 = df1.join(ph, $"p_termID" === $"termID" && $"p_docID" === $"docID", "leftouter")
df2.cache()

val train = sqlContext.read.parquet(work_dir+"gov2_ph_100K_OR_bins.parquet").
toDF("tr_termID","tr_docID","label_top10","label_top1k")

val df = df2.join(train, $"tr_termID" === $"termID" && $"tr_docID" === $"docID", "leftouter").
select("termID","docID","term_freq","doc_term_freq","bm25",
"d_len","d_terms","d_xdoc","d_doc_ratio","d_xdoc_norm1","d_xdoc_norm2",
"d_b1","d_b2","d_b3","d_b4","d_b5","d_b6","d_b7","d_b8","d_b9","d_b10","d_b20","d_b40","d_b80","d_b160","d_b320","d_b640","d_b1280",
"d_top10","d_top20","d_top40","d_top80","d_top160","d_top1k",
"p_b1","p_b2","p_b3","p_b4","p_b5","p_b6","p_b7","p_b8","p_b9","p_b10","p_b20","p_b40","p_b80","p_b160","p_b320","p_b640","p_b1280",
"p_top10","p_top20","p_top40","p_top80","p_top160","p_top1k",
"label_top10","label_top1k").na.fill(0)

df.write.format("com.databricks.spark.csv").option("header","true").save(work_dir+"gov2-train-features-b.csv")
df.write.parquet(work_dir+"gov2-train-features-b.parquet")

// 7. train 
//----------------------------------------------------------------------------------------------------------------

 
sc.setLogLevel("ERROR")
val work_dir = "/home/juan/save/"
val toDouble = udf[Double, String]( _.toDouble)
val toDouble = udf[Double, Int]( _.toDouble)

val df = sqlContext.read.parquet(work_dir+"gov2-train-features-b.parquet").
select("label_top1k","label_top10","term_freq","doc_term_freq","bm25",
"d_len","d_terms","d_xdoc","d_doc_ratio","d_xdoc_norm1","d_xdoc_norm2",
"d_b1","d_b2","d_b3","d_b4","d_b5","d_b6","d_b7","d_b8","d_b9","d_b10","d_b20","d_b40","d_b80","d_b160","d_b320","d_b640","d_b1280",
"d_top10","d_top20","d_top40","d_top80","d_top160","d_top1k",
"p_b1","p_b2","p_b3","p_b4","p_b5","p_b6","p_b7","p_b8","p_b9","p_b10","p_b20","p_b40","p_b80","p_b160","p_b320","p_b640","p_b1280",
"p_top10","p_top20","p_top40","p_top80","p_top160","p_top1k").
withColumn("label_top1k",toDouble($"label_top1k")).
withColumn("label_top10",toDouble($"label_top10")).
withColumn("term_freq",toDouble($"term_freq")).
withColumn("doc_term_freq",toDouble($"doc_term_freq")).
withColumn("bm25",toDouble($"bm25")).
withColumn("d_len",toDouble($"d_len")).
withColumn("d_terms",toDouble($"d_terms")).
withColumn("d_xdoc",toDouble($"d_xdoc")).
withColumn("d_b1",toDouble($"d_b1")).
withColumn("d_b2",toDouble($"d_b2")).
withColumn("d_b3",toDouble($"d_b3")).
withColumn("d_b4",toDouble($"d_b4")).
withColumn("d_b5",toDouble($"d_b5")).
withColumn("d_b6",toDouble($"d_b6")).
withColumn("d_b7",toDouble($"d_b7")).
withColumn("d_b8",toDouble($"d_b8")).
withColumn("d_b9",toDouble($"d_b9")).
withColumn("d_b10",toDouble($"d_b10")).
withColumn("d_b20",toDouble($"d_b20")).
withColumn("d_b40",toDouble($"d_b40")).
withColumn("d_b80",toDouble($"d_b80")).
withColumn("d_b160",toDouble($"d_b160")).
withColumn("d_b320",toDouble($"d_b320")).
withColumn("d_b640",toDouble($"d_b640")).
withColumn("d_b1280",toDouble($"d_b1280")).
withColumn("d_top10",toDouble($"d_top10")).
withColumn("d_top20",toDouble($"d_top20")).
withColumn("d_top40",toDouble($"d_top40")).
withColumn("d_top80",toDouble($"d_top80")).
withColumn("d_top160",toDouble($"d_top160")).
withColumn("d_top1k",toDouble($"d_top1k")).
withColumn("p_b1",toDouble($"p_b1")).
withColumn("p_b2",toDouble($"p_b2")).
withColumn("p_b3",toDouble($"p_b3")).
withColumn("p_b4",toDouble($"p_b4")).
withColumn("p_b5",toDouble($"p_b5")).
withColumn("p_b6",toDouble($"p_b6")).
withColumn("p_b7",toDouble($"p_b7")).
withColumn("p_b8",toDouble($"p_b8")).
withColumn("p_b9",toDouble($"p_b9")).
withColumn("p_b10",toDouble($"p_b10")).
withColumn("p_b20",toDouble($"p_b20")).
withColumn("p_b40",toDouble($"p_b40")).
withColumn("p_b80",toDouble($"p_b80")).
withColumn("p_b160",toDouble($"p_b160")).
withColumn("p_b320",toDouble($"p_b320")).
withColumn("p_b640",toDouble($"p_b640")).
withColumn("p_b1280",toDouble($"p_b1280")).
withColumn("p_top10",toDouble($"p_top10")).
withColumn("p_top20",toDouble($"p_top20")).
withColumn("p_top40",toDouble($"p_top40")).
withColumn("p_top80",toDouble($"p_top80")).
withColumn("p_top160",toDouble($"p_top160")).
withColumn("p_top1k",toDouble($"p_top1k")).
na.fill(0.0).cache()

//val dataTop1k = df.drop("label_top10").
//dataTop1k.write.parquet(work_dir+"top1k.ph.train.parquet")

val dataTop10 = df.drop("label_top1k").
dataTop1k.write.parquet(work_dir+"top10.ph.train.parquet")

// learn
// -----
sc.setLogLevel("ERROR")
val work_dir = "/home/juan/save/"

import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.regression.RandomForestRegressionModel
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.Row
import org.apache.spark.ml.tuning
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}
import org.apache.spark.ml.evaluation._
import org.apache.spark.ml.param.ParamMap


val model_name="top1k"
val data = sqlContext.read.parquet(work_dir+model_name+".ph.train.parquet").na.fill(0.0).map(row => { 
val d = (for (i<-1 to 54) yield row.getDouble(i)).toArray
(row.getDouble(0), Vectors.dense(d)) 
}).toDF("label","features")

val splits = data.randomSplit(Array(0.7, 0.3))
val (trainingData, testData) = (splits(0), splits(1))

val rf = new RandomForestRegressor().setNumTrees(50).setMaxBins(20).setMaxDepth(20)
val evaluator = new RegressionEvaluator()
val cv = new CrossValidator().
setEstimator(rf).
setEvaluator(evaluator).
setEstimatorParamMaps(Array(rf.extractParamMap))

val model = cv.fit(trainingData)
model.save(work_dir+"top1k_drf.model")

//val sameModel = RandomForestModel.load(sc, "target/tmp/myRandomForestRegressionModel")

