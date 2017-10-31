// STATIC PRUNE PIPELINE

sc.setLogLevel("ERROR")

val ph = spark.read
  .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
  .option("header", false).load(phFn)
  .repartition(spark.sparkContext.defaultParallelism * 3)
  .toDF("termID", "docID", "ph_b1", "ph_b2", "ph_b3", "ph_b4", "ph_b5", "ph_b6", "ph_b7", "ph_b8", "ph_b9", "ph_b10",
    "ph_b20", "ph_b40", "ph_b80", "ph_b160", "ph_b320", "ph_b640", "ph_b1280", "ph_top10","ph_top1k")
  .withColumn("termID", $"termID".cast("Int"))
  .withColumn("docID", $"docID".cast("Int"))
  .withColumn("ph_b1", $"ph_b1".cast("Int"))
  .withColumn("ph_b2", $"ph_b2".cast("Int"))
  .withColumn("ph_b3", $"ph_b3".cast("Int"))
  .withColumn("ph_b4", $"ph_b4".cast("Int"))
  .withColumn("ph_b5", $"ph_b5".cast("Int"))
  .withColumn("ph_b6", $"ph_b6".cast("Int"))
  .withColumn("ph_b7", $"ph_b7".cast("Int"))
  .withColumn("ph_b8", $"ph_b8".cast("Int"))
  .withColumn("ph_b9", $"ph_b9".cast("Int"))
  .withColumn("ph_b10", $"ph_b10".cast("Int"))
  .withColumn("ph_b20", $"ph_b20".cast("Int"))
  .withColumn("ph_b40", $"ph_b40".cast("Int"))
  .withColumn("ph_b80", $"ph_b80".cast("Int"))
  .withColumn("ph_b160", $"ph_b160".cast("Int"))
  .withColumn("ph_b320", $"ph_b320".cast("Int"))
  .withColumn("ph_b640", $"ph_b640".cast("Int"))
  .withColumn("ph_b1280", $"ph_b1280".cast("Int"))
  .withColumn("ph_top10", $"ph_top10".cast("Int"))
  .withColumn("ph_top1k", $"ph_top1k".cast("Int"))

ph.write
  .format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
  .save("ph.parquet")

val d = spark.read
  .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
  .option("header", false)
  .load(dFn)
  .repartition(spark.sparkContext.defaultParallelism * 3)
  .toDF("docID", "d_docSize", "d_docTerms", "d_xdoc")
  .withColumn("docID", $"docID".cast("Int"))
  .withColumn("d_docSize", $"d_docSize".cast("Int"))
  .withColumn("d_docTerms", $"d_docTerms".cast("Int"))
  .withColumn("d_xdoc", $"d_xdoc".cast("Double"))
  .withColumn("d_doc_s_t", $"d_docSize".cast("Double") / $"d_docTerms")
  .withColumn("d_xdoc_s", $"d_xdoc" / $"d_docSize")
  .withColumn("d_xdoc_t", $"d_xdoc" / $"d_docTerms")

d.write.parquet("d.parquet")

val dh = spark.read
  .format("org.apache.spark.sql.execution.datasources.csv.CSVFileFormat")
  .option("header", false)
  .load("dh.csv")
  .repartition(spark.sparkContext.defaultParallelism * 3)
  .withColumn("docID", $"_c0".cast("Int"))
  .withColumn("dh_b1", $"_c1".cast("Int"))
  .withColumn("dh_b2", $"_c2".cast("Int"))
  .withColumn("dh_b3", $"_c3".cast("Int"))
  .withColumn("dh_b4", $"_c4".cast("Int"))
  .withColumn("dh_b5", $"_c5".cast("Int"))
  .withColumn("dh_b6", $"_c6".cast("Int"))
  .withColumn("dh_b7", $"_c7".cast("Int"))
  .withColumn("dh_b8", $"_c8".cast("Int"))
  .withColumn("dh_b9", $"_c9".cast("Int"))
  .withColumn("dh_b10", $"_c10".cast("Int"))
  .withColumn("dh_b20", $"_c11".cast("Int"))
  .withColumn("dh_b40", $"_c12".cast("Int"))
  .withColumn("dh_b80", $"_c13".cast("Int"))
  .withColumn("dh_b160", $"_c14".cast("Int"))
  .withColumn("dh_b320", $"_c15".cast("Int"))
  .withColumn("dh_b640", $"_c16".cast("Int"))
  .withColumn("dh_b1280", $"_c17".cast("Int"))
  .withColumn("dh_top10", $"_c18".cast("Int"))
  .withColumn("dh_top1k", $"_c19".cast("Int"))

dh.write
      .format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
      .save("dh.parquet")

sc.setLogLevel("ERROR")

val d = spark.read.parquet("d.parquet")
    .repartition(spark.sparkContext.defaultParallelism * 3)

val dh = spark.read.parquet("dh.parquet")
    .repartition(spark.sparkContext.defaultParallelism * 3)

val d1 = d.join(dh,Seq("docID"),"leftouter")
    .withColumn("dh_ds_top10",$"dh_top10".cast("Double")/$"d_docSize")
    .withColumn("dh_ds_top1K",$"dh_top1K".cast("Double")/$"d_docSize")
    .withColumn("dh_dt_top10",$"dh_top10".cast("Double")/$"d_docTerms")
    .withColumn("dh_dt_top1K",$"dh_top1K".cast("Double")/$"d_docTerms")

val p = spark.read.parquet("p.parquet")
    .withColumnRenamed("p_termID","termID")
    .withColumnRenamed("p_docID","docID")
    .repartition(spark.sparkContext.defaultParallelism * 3)

val pt = spark.read.parquet("pt.parquet")
    .withColumnRenamed("pt_termID","termID")
    .repartition(spark.sparkContext.defaultParallelism * 3)

val ph = spark.read.parquet("ph.parquet")
    .withColumnRenamed("ph_termID","termID")
    .withColumnRenamed("ph_docID","docID")
    .repartition(spark.sparkContext.defaultParallelism * 3)

val p1 = p.join(pt,Seq("termID"),"leftouter")

val p2 = p1.join(ph,Seq("termID","docID"),"leftouter")
    .withColumn("ph_tf_top10",$"ph_top10".cast("Double")/$"p_tfreq")
    .withColumn("ph_tf_top1K",$"ph_top1K".cast("Double")/$"p_tfreq")
    .withColumn("ph_tdf_top10",$"ph_top10".cast("Double")/$"p_tdfreq")
    .withColumn("ph_tdf_top1K",$"ph_top1K".cast("Double")/$"p_tdfreq")

val df = p2.join(d1,Seq("docID"),"leftouter")

df.write.parquet("cw09b.features.parquet")

val lph = spark.read.option("header","false").csv("/mnt/research/data/cw09b/100K.OR.csv-0.ph.txt")
     .withColumnRenamed("_c0","termID").withColumn("termID",$"termID".cast("Int"))
     .withColumnRenamed("_c1","docID").withColumn("docID",$"docID".cast("Int"))
     .withColumn("b1",$"_c2".cast("Int"))
     .withColumn("b2",$"_c3".cast("Int"))
     .withColumn("b3",$"_c4".cast("Int"))
     .withColumn("b4",$"_c5".cast("Int"))
     .withColumn("b5",$"_c6".cast("Int"))
     .withColumn("b6",$"_c7".cast("Int"))
     .withColumn("b7",$"_c8".cast("Int"))
     .withColumn("b8",$"_c9".cast("Int"))
     .withColumn("b9",$"_c10".cast("Int"))
     .withColumn("b10",$"_c11".cast("Int"))
     .withColumn("b20",$"_c12".cast("Int"))
     .withColumn("b40",$"_c13".cast("Int"))
     .withColumn("b80",$"_c14".cast("Int"))
     .withColumn("b160",$"_c15".cast("Int"))
     .withColumn("b320",$"_c16".cast("Int"))
     .withColumn("b640",$"_c17".cast("Int"))
     .withColumn("b1k",$"_c18".cast("Int"))
     .withColumn("top10",$"b1" + $"b2" + $"b3" + $"b4" + $"b5" + $"b6" + $"b7" + $"b8" + $"b9" + $"b10")
     .withColumn("top1k",$"top10" + $"b20" + $"b40" + $"b80" + $"b160" + $"b320" + $"b640" + $"b1k")
     .select("termID","docID","top10","top1k")
     .filter("top1k>0")

lph.write.parquet("/mnt/research/data/cw09b/100K.OR.ph.parquet")

val ldh = spark.read.option("header","false").csv("/mnt/research/data/cw09b/100K.OR.csv-0.dh.txt")
     .withColumnRenamed("_c0","docID").withColumn("docID",$"docID".cast("Int"))
     .withColumn("b1",$"_c1".cast("Int"))
     .withColumn("b2",$"_c2".cast("Int"))
     .withColumn("b3",$"_c3".cast("Int"))
     .withColumn("b4",$"_c4".cast("Int"))
     .withColumn("b5",$"_c5".cast("Int"))
     .withColumn("b6",$"_c6".cast("Int"))
     .withColumn("b7",$"_c7".cast("Int"))
     .withColumn("b8",$"_c8".cast("Int"))
     .withColumn("b9",$"_c9".cast("Int"))
     .withColumn("b10",$"_c10".cast("Int"))
     .withColumn("b20",$"_c11".cast("Int"))
     .withColumn("b40",$"_c12".cast("Int"))
     .withColumn("b80",$"_c13".cast("Int"))
     .withColumn("b160",$"_c14".cast("Int"))
     .withColumn("b320",$"_c15".cast("Int"))
     .withColumn("b640",$"_c16".cast("Int"))
     .withColumn("b1k",$"_c17".cast("Int"))
     .withColumn("top10",$"b1" + $"b2" + $"b3" + $"b4" + $"b5" + $"b6" + $"b7" + $"b8" + $"b9" + $"b10")
     .withColumn("top1k",$"top10" + $"b20" + $"b40" + $"b80" + $"b160" + $"b320" + $"b640" + $"b1k")
     .select("docID","top10","top1k")
     .filter("top1k>0")

dph.write.parquet("/mnt/research/data/cw09b/100K.OR.dh.parquet")

// ----------------------------------------------------

val lph = spark.read.parquet("100K.OR.ph.parquet")

val lph_1k = lph.filter("top1k>0")
    .select("termID","docID","top1k")
    .groupBy("termID","docID")
    .agg(sum("top1k") as "label")
    .select("termID","docID","label")

val lph_10 = lph.filter("top10>0")
    .select("termID","docID","top10")
    .groupBy("termID","docID")
    .agg(sum("top10") as "label")
    .select("termID","docID","label")


val ldh = spark.read.parquet("100K.OR.dh.parquet")

val ldh_1k = ldh.filter("top1k>0")
    .select("docID","top1k")
    .groupBy("docID")
    .agg(sum("top1k") as "label")
    .select("docID","label")

val ldh_10 = ldh.filter("top10>0")
    .select("docID","top10")
    .groupBy("docID")
    .agg(sum("top10") as "label")
    .select("docID","label")

ldh_1k.write.parquet("ldh_1k.parquet")
ldh_10.write.parquet("ldh_10.parquet")

//-----------------------------------------------------------------

val df = spark.read.parquet("features.parquet")

val lph_1k = spark.read.parquet("lph_1k.parquet")
val pos_ph_1k = df.join(lph_1k,Seq("termID","docID"),"inner")
pos_ph_1k.write.parquet("pos_ph_1k.parquet")

val lph_10 = spark.read.parquet("lph_10.parquet")
val pos_ph_10 = df.join(lph_10,Seq("termID","docID"),"inner")
pos_ph_10.write.parquet("pos_ph_10.parquet")

val ldh_1k = spark.read.parquet("ldh_1k.parquet")
val pos_dh_1k = df.join(ldh_1k,Seq("termID","docID"),"inner")
pos_dh_1k.write.parquet("pos_dh_1k.parquet")

val ldh_10 = spark.read.parquet("ldh_10.parquet")
val pos_dh_10 = df.join(ldh_10,Seq("termID","docID"),"inner")
pos_dh_10.write.parquet("pos_dh_10.parquet")

// negative exmaples
val neg1k = df.sample(false,0.05)
    .join(pos_ph_1k.select("termID","docID","label"),Seq("termID","docID"),"leftouter")
    .filter("label is null")
    .sample(false,0.5)

val train_ph_1k = pos_ph_1k.unionAll(neg1k)
train_ph_1k.write.parquet("train_ph_1k.parquet")


// -------------------------------------------------------------------------
// feature engineering

sc.setLogLevel("ERROR")

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.feature._
import org.apache.spark.ml.Pipeline
import scala.collection.mutable._

val id = Array("termID","docID")

val features = Array(
  "d_docSize","d_docTerms","d_xdoc","d_doc_s_t","d_xdoc_s","d_xdoc_t",
  "p_tfreq","p_tdfreq","p_bm25","pt_pt",
  "dh_b1","dh_b2","dh_b3","dh_b4","dh_b5","dh_b6","dh_b7","dh_b8","dh_b9","dh_b10",
  "dh_b20","dh_b40","dh_b80","dh_b160","dh_b320","dh_b640","dh_b1280",
  "dh_top10","dh_top1k","dh_ds_top10","dh_ds_top1K","dh_dt_top10","dh_dt_top1K",
  "ph_b1","ph_b2","ph_b3","ph_b4","ph_b5","ph_b6","ph_b7","ph_b8","ph_b9","ph_b10",
  "ph_b20","ph_b40","ph_b80","ph_b160","ph_b320","ph_b640","ph_b1280","ph_top10","ph_top1k",
  "ph_tf_top10","ph_tf_top1K","ph_tdf_top10","ph_tdf_top1K"
)

val docFeatures = Array("d_docSize","d_docTerms","d_xdoc","d_doc_s_t","d_xdoc_s","d_xdoc_t")

val termFeatures = Array("p_tfreq","p_tdfreq","p_bm25", "pt_pt")

val dhFeatures = Array("dh_b1","dh_b2","dh_b3","dh_b4","dh_b5","dh_b6","dh_b7","dh_b8","dh_b9","dh_b10",
  "dh_b20","dh_b40","dh_b80","dh_b160","dh_b320","dh_b640","dh_b1280",
  "dh_top10","dh_top1k","dh_ds_top10","dh_ds_top1K","dh_dt_top10","dh_dt_top1K")

val phFeatures = Array("ph_b1","ph_b2","ph_b3","ph_b4","ph_b5","ph_b6","ph_b7","ph_b8","ph_b9","ph_b10",
  "ph_b20","ph_b40","ph_b80","ph_b160","ph_b320","ph_b640","ph_b1280",
  "ph_top10","ph_top1k","ph_tf_top10","ph_tf_top1K","ph_tdf_top10","ph_tdf_top1K")

val intrinsic_features = docFeatures ++ termFeatures
val docbased_features = docFeatures ++ dhFeatures
val termbased_features = termFeatures ++ phFeatures

val all = (id ++ features)

val featureGroups = Map(
  "doc" -> (id ++ docFeatures),
  "term" -> (id ++ termFeatures),
  "ph" -> (id ++ phFeatures),
  "dh" -> (id ++dhFeatures),
  "termbased" -> (id ++ termbased_features),
  "intrinsic" -> (id ++ intrinsic_features),
  "docbased" -> (id ++ docbased_features)
)

val saveFile = "/mnt/work2/train_ph_1k"

val t = spark.read.parquet("/mnt/work2/train_ph_1k.parquet").withColumn("label",$"label".cast("Double")).na.fill(0.0)

// all features
val stage = new VectorAssembler().setInputCols(all).setOutputCol("features")
val d1 = stage.transform(t).select("label","features")
d1.write.parquet(saveFile + ".all.features.parquet")
val d2 = spark.read.parquet(saveFile + ".all.features.parquet").withColumn("label",$"label".cast("Double"))
val Array(train, test, eval) = d2.randomSplit(Array(0.7,0.15,0.15), seed = 12345)
train.repartition(1).write.format("libsvm").save("/mnt/work2/train_ph_1k.all.train.libsvm")
test.repartition(1).write.format("libsvm").save("/mnt/work2/train_ph_1k.all.test.libsvm")
eval.repartition(1).write.format("libsvm").save("/mnt/work2/train_ph_1k.all.eval.libsvm")


featureGroups.foreach { case (key, value) =>
  val stage = new VectorAssembler().setInputCols(value).setOutputCol("features")
  val d1 = stage.transform(t).select("label","features")
  d1.write.parquet(saveFile + "." + key + ".features.parquet")
  val d2 = spark.read.parquet(saveFile + "." + key + ".features.parquet")
  val Array(train, test, eval) = d2.randomSplit(Array(0.7,0.15,0.15), seed = 12345)
//  train.repartition(1).write.format("libsvm").save("/mnt/work2/train_ph_1k." + key + ".train.libsvm")
//  test.repartition(1).write.format("libsvm").save("/mnt/work2/train_ph_1k." + key + ".test.libsvm")
//  eval.repartition(1).write.format("libsvm").save("/mnt/work2/train_ph_1k." + key + ".eval.libsvm")
  train.write.format("libsvm").save("/mnt/work2/train_ph_1k." + key + ".train.libsvm")
  test.write.format("libsvm").save("/mnt/work2/train_ph_1k." + key + ".test.libsvm")
  eval.write.format("libsvm").save("/mnt/work2/train_ph_1k." + key + ".eval.libsvm")

}

// get a vector field names

df.select("features").schema.fields(0).metadata.getMetadata("ml_attr").getMetadata("attrs").getMetadataArray("numeric").foreach(r => println(r.getString("name")))

// get vector columns
df.select("features").map{case Row(v: Vector) => (v(0), v(1))}.toDF("termID","docID").withColumn("termID", $"termID".cast("Int")).withColumn("docID",$"docID".cast("Int"))


//----------------------------------------------------------------------------------------
// combine predictions with posting IDs

sc.setLogLevel("ERROR")

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.SaveMode
import spark.sqlContext.implicits._
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.feature._
import java.io.File
import org.apache.hadoop.fs._;


// get files by extension
// TODO: change to find files in HDFS, not local FS
def getListOfFiles(dir: File, extensions: List[String]): List[File] = {
    dir.listFiles.filter(_.isFile).toList.filter { file =>
        extensions.exists(file.getName.endsWith(_))
    }
}

// get files by pattern and extension
// TODO: change to find files in HDFS, not local FS
def getFiles(dir: File, pattern: String, extensions: List[String]): List[File] = {
    dir.listFiles.filter(_.isFile).toList.filter { file =>
        (file.getName contains pattern) && extensions.exists(file.getName.endsWith(_))
    }
}

// add unique index column (row number)
def addIndex(df: org.apache.spark.sql.DataFrame) = spark.sqlContext.createDataFrame(
  // Add index
  df.rdd.zipWithIndex.map{case (r, i) => Row.fromSeq(r.toSeq :+ i)},
  // Create schema
  StructType(df.schema.fields :+ StructField("_index", LongType, false))
)

val dir = "/home/juan/cw09b.features.libsvm/"
val files = getListOfFiles(new File(dir), List("libsvm"))

val predictDir = "/mnt/research/experiments/train/predict/"
val outDir = "/mnt/work2/prediction/"
val fs = FileSystem.get(sc.hadoopConfiguration);

for (f <- files) {

  val partName = f.getName.split("-")(1)
  val df = spark.read.format("libsvm").option("numFeatures",58).load(f.getAbsolutePath)
  val features = addIndex(df.select("features").map{case Row(v: Vector) => (v(0), v(1))}.toDF("termID","docID").withColumn("termID", $"termID".cast("Int")).withColumn("docID",$"docID".cast("Int")))
  features.persist()

  val predictFiles = getListOfFiles(new File(predictDir), List(partName.concat(".predict.txt")))
  if (predictFiles.size > 0) {
    for (pf <- predictFiles) {

      print(partName)
      print(" : ")
      print(pf.getName)

      val prefix = pf.getName.split("\\.")(0)
      val outName = outDir.concat(prefix)
      val outFile = new Path(outName + "/" + partName + ".csv")

      if (!fs.exists(outFile)) {

        val scores = addIndex(spark.read.csv(pf.getAbsolutePath).withColumn("predicted",$"_c0".cast("Double"))).select("predicted","_index")
        val result = features.join(scores, Seq("_index"), "inner").drop("_index")
        result.coalesce(1).write.mode(SaveMode.Append).option("header","false").csv(path=outName)

        val f = fs.globStatus(new Path(outName + "/part-000*"))(0).getPath.getName
        fs.rename(new Path(outName + "/" + f), outFile)
      }
      else {
        print(" - skipping...")
      }
      println()

    }
  }
  else {
    println("missing: ".concat(partName))
  }

  features.unpersist()
}


//-----------------------------------------------------------------------------------------------

sc.setLogLevel("ERROR")
//val models = Array("all","doc","term","posthits","dochits","intrinsic")
val models = Array("all","dochits")
for (m <- models) {
  val d = spark.read.option("header","false").csv("/mnt/work2/prediction/" + m + "/*.csv")
    .toDF("termID","docID","top1k")
    .withColumn("top1k",$"top1k".cast("double"))
  d.orderBy(desc("top1k")).write.parquet("/mnt/work2/prediction/" + m + ".top1k.parquet")
}





