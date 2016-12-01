import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.Row
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util._

// generate full gov2 features
// join index-features + fake-query posthit features + fake query dochits features + training queries posthits features
// -----------------------------------------------------------------------------------------------------------------------

sc.setLogLevel("ERROR")

val work_dir = "/home/juan/work/save/"
// val toInt    = udf[Int, String]( _.toInt)
// val toDouble = udf[Double, String]( _.toDouble)

val index_features = sqlContext.read.parquet(work_dir+"gov2_index_features.parquet")

val dh = sqlContext.read.parquet(work_dir+"gov2_dh_features.parquet")
	.toDF("d_docID","d_len","d_terms","d_xdoc","d_doc_ratio","d_xdoc_norm1","d_xdoc_norm2","d_b1","d_b2","d_b3","d_b4","d_b5","d_b6","d_b7","d_b8","d_b9","d_b10","d_b20","d_b40","d_b80","d_b160","d_b320","d_b640","d_b1280","d_top10","d_top20","d_top40","d_top80","d_top160","d_top1k")

val ph = sqlContext.read.parquet(work_dir+"gov2_ph_OR_60M_bins.parquet")
	.toDF("p_termID","p_docID","p_b1","p_b2","p_b3","p_b4","p_b5","p_b6","p_b7","p_b8","p_b9","p_b10","p_b20","p_b40","p_b80","p_b160","p_b320","p_b640","p_b1280","p_top10","p_top20","p_top40","p_top80","p_top160","p_top1k")

val df1 = index_features.join(dh, $"d_docID" === $"docID", "inner")

val df2 = df1.join(ph, $"p_termID" === $"termID" && $"p_docID" === $"docID", "leftouter")
	.select("term_freq","doc_term_freq","bm25","d_len","d_terms","d_xdoc","d_doc_ratio","d_xdoc_norm1","d_xdoc_norm2","d_b1","d_b2","d_b3","d_b4","d_b5","d_b6","d_b7","d_b8","d_b9","d_b10","d_b20","d_b40","d_b80","d_b160","d_b320","d_b640","d_b1280","d_top10","d_top20","d_top40","d_top80","d_top160","d_top1k","p_b1","p_b2","p_b3","p_b4","p_b5","p_b6","p_b7","p_b8","p_b9","p_b10","p_b20","p_b40","p_b80","p_b160","p_b320","p_b640","p_b1280","p_top10","p_top20","p_top40","p_top80","p_top160","p_top1k","termID","docID")
	.na.fill(0)

df2.write.parquet(work_dir+"gov2-full-features-b.parquet")

//--------------------------------------------------------------
sc.setLogLevel("ERROR")
val toDouble = udf[Double, Int]( _.toDouble)
val work_dir = "/home/juan/work/save/"
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.Row
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util._
val data = sqlContext.read.parquet(work_dir+"gov2-full-features-b.parquet").
withColumn("term_freq",toDouble($"term_freq")).
withColumn("doc_term_freq",toDouble($"doc_term_freq")).
withColumn("d_len",toDouble($"d_len")).
withColumn("d_terms",toDouble($"d_terms")).
withColumn("d_b1",toDouble($"d_b1")).
withColumn("d_b2",toDouble($"d_b2")).
withColumn("d_b3",toDouble($"d_b5")).
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
withColumn("p_b10",toDouble($"p_b40")).
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
withColumn("termID",toDouble($"termID")).
withColumn("docID",toDouble($"docID")).
map(row => {
	val d = (for (i<-0 to 56) yield row.getDouble(i)).toArray
	LabeledPoint(0.toDouble, Vectors.dense(d).toSparse)
})
//.repartition(20)
//.randomSplit(Array(0.6, 0.2, 0.2), seed = 11L)

// val training = data(0)
// val test = data(1)
// val eval = data(2)

// MLUtils.saveAsLibSVMFile(training,work_dir+model_name+".ph.train.libsvm")

MLUtils.saveAsLibSVMFile(data,work_dir+"gov2-ph-full.libsvm")
