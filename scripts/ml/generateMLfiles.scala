import scala.collection.mutable._
import org.apache.spark.sql.Row
import org.apache.spark.ml.feature._
import org.apache.spark.mllib.linalg._
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.mllib.util.MLUtils.saveAsLibSVMFile

sc.setLogLevel("ERROR")

val workDir = "/home/juan/work/workspace/"

// feature groups
//---------------

val raw_features = Array("d_docSize","d_docTerms","d_xdoc","d_doc_s_t","d_xdoc_s","d_xdoc_t",
  "p_tfreq","p_tdfreq","p_bm25",
  "ph_b1","ph_b2","ph_b3","ph_b4","ph_b5","ph_b6","ph_b7","ph_b8","ph_b9","ph_b10",
  "ph_b20","ph_b40","ph_b80","ph_b160","ph_b320","ph_b640","ph_b1280","ph_top10","ph_top1K",
  "ph_tf_top10","ph_tf_top1K","ph_tdf_top10","ph_tdf_top1K",
  "dh_b1","dh_b2","dh_b3","dh_b4","dh_b5","dh_b6","dh_b7","dh_b8","dh_b9","dh_b10",
  "dh_b20","dh_b40","dh_b80","dh_b160","dh_b320","dh_b640","dh_b1280",
  "dh_top10","dh_top1K","dh_ds_top10","dh_ds_top1K","dh_dt_top10","dh_dt_top1K"
)

val docFeatures = Array("d_docSize","d_docTerms","d_xdoc","d_doc_s_t","d_xdoc_s","d_xdoc_t")

val termFeatures = Array("p_tfreq","p_tdfreq","p_bm25")

val dhFeatures = Array("dh_b1","dh_b2","dh_b3","dh_b4","dh_b5","dh_b6","dh_b7","dh_b8","dh_b9","dh_b10",
  "dh_b20","dh_b40","dh_b80","dh_b160","dh_b320","dh_b640","dh_b1280",
  "dh_top10","dh_top1K","dh_ds_top10","dh_ds_top1K","dh_dt_top10","dh_dt_top1K")

val phFeatures = Array("ph_b1","ph_b2","ph_b3","ph_b4","ph_b5","ph_b6","ph_b7","ph_b8","ph_b9","ph_b10",
  "ph_b20","ph_b40","ph_b80","ph_b160","ph_b320","ph_b640","ph_b1280",
  "ph_top10","ph_top1K","ph_tf_top10","ph_tf_top1K","ph_tdf_top10","ph_tdf_top1K")

val intrinsic_features = docFeatures ++ termFeatures
val docbased_features = docFeatures ++ dhFeatures
val termbased_features = termFeatures ++ phFeatures

//-----------------------------

//val df = spark.read.parquet(workDir+"cw09b.features.sample.parquet")
//val df = spark.read.parquet(workDir+"cw09b.features.full.parquet")

var labels = Array("l1k_top10","l1k_top1K","l10_top10","l10_top1K")

// generate feature groups
val featureGroups = Map(
  "doc" -> docFeatures,
  "term" -> termFeatures,
  "dh" -> dhFeatures,
  "ph" -> phFeatures,
  "intrinsic" -> intrinsic_features,
  "docbased" -> docbased_features,
  "termbased" -> termbased_features
)

// this requires more than twice the disk space & I/O shuffling, since dataset is wider until the end
// create all feature groups
//val stages = ArrayBuffer[VectorAssembler]()
//val pipeline = new Pipeline().setStages(stages.toArray)
//val model = pipeline.fit(df)
//val d = model.transform(df).drop(raw_features :_*).write.parquet(workDir+"cw09b.full.feature_groups.parquet")

// do each group separately
//featureGroups.foreach { case (key, value) =>
//  val stage = new VectorAssembler().setInputCols(value).setOutputCol(key)
//  stage.transform(df).drop(raw_features :_*).write.parquet(workDir+"cw09b.full."+key+".parquet")
//}

val ds = spark.read.parquet(workDir+"cw09b.full.all.parquet")
labels.foreach { label =>
  val r = ds.map(row => row.getAs[Double](label) -> row.getAs[org.apache.spark.ml.linalg.SparseVector]("features").toDense.toArray)
  var r1 = r.map(row => org.apache.spark.mllib.regression.LabeledPoint(row._1, org.apache.spark.mllib.linalg.Vectors.dense(row._2).toSparse))
  saveAsLibSVMFile(r1.rdd, "/home/juan/work/work-sandbox/" + "cw09b.full.all."+label+".libsvm")
}

featureGroups.foreach { case (key, value) =>
  val d = spark.read.parquet(workDir + "cw09b.full." + key + ".parquet")
  labels.foreach { label =>
    val r = d.map(row => row.getAs[Double](label) -> row.getAs[org.apache.spark.ml.linalg.SparseVector]("features").toDense.toArray)
    var r1 = r.map(row => org.apache.spark.mllib.regression.LabeledPoint(row._1, org.apache.spark.mllib.linalg.Vectors.dense(row._2).toSparse))
    saveAsLibSVMFile(r1.rdd, "/home/juan/work/work-sandbox/" + "cw09b.full" + key + "." +label+".libsvm")
  }
}




