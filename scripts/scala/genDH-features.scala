import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.Row
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util._

val toDouble = udf[Double, Int]( _.toDouble)

var top10 = sqlContext.read.parquet("/home/juan/work/save/gov2_dh_features.parquet").
  na.fill(0.0).
  withColumn("len",toDouble($"len")).
  withColumn("terms",toDouble($"terms")).
  withColumn("b1",toDouble($"b1")).
  withColumn("b2",toDouble($"b2")).
  withColumn("b3",toDouble($"b3")).
  withColumn("b4",toDouble($"b4")).
  withColumn("b5",toDouble($"b5")).
  withColumn("b6",toDouble($"b6")).
  withColumn("b7",toDouble($"b7")).
  withColumn("b8",toDouble($"b8")).
  withColumn("b9",toDouble($"b9")).
  withColumn("b10",toDouble($"b10")).
  withColumn("b20",toDouble($"b20")).
  withColumn("b40",toDouble($"b40")).
  withColumn("b80",toDouble($"b80")).
  withColumn("b160",toDouble($"b160")).
  withColumn("b320",toDouble($"b320")).
  withColumn("b640",toDouble($"b640")).
  withColumn("b1280",toDouble($"b1280")).
  withColumn("top10",toDouble($"top10")).
  select("top10","len","terms","xdoc","doc_ratio","xdoc_norm1","xdoc_norm2",
    "b1","b2","b3","b4","b5","b6","b7","b8","b9","b10",
    "b20","b40","b80","b160","b320","b640","b1280")

  var data = top10.
  map(row => {
    val d = (for (i<-1 to 23) yield row.getDouble(i)).toArray
    LabeledPoint(row.getDouble(0), Vectors.dense(d).toSparse)
  })
//    .
//  toDF("label","features")

val splits = data.randomSplit(Array(0.75, 0.25))

MLUtils.saveAsLibSVMFile(splits(0),"gov2-dh-top10-train.libsvm")
MLUtils.saveAsLibSVMFile(splits(1),"gov2-dh-top10-test.libsvm")

