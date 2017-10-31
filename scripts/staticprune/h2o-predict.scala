import org.apache.spark._
import java.io._
import _root_.hex.genmodel.easy.RowData
import _root_.hex.genmodel.easy.EasyPredictModelWrapper
import _root_.hex.genmodel.easy.prediction._
import _root_.hex.genmodel.MojoModel
import org.apache.spark.sql.Row


val modelName = "/mnt/scratch/gbm_top1k_all.zip"
val fileName = "/mnt/scratch/cw09b/cw09b.features.parquet"

sc.setLogLevel("ERROR")
import spark.implicits._

val features = Array(
  "docID" ,
  "p_tfreq", "p_tdfreq", "p_bm25","pt_pt",
  "d_docSize", "d_docTerms", "d_xdoc", "d_doc_s_t", "d_xdoc_s", "d_xdoc_t",
  "dh_b1", "dh_b2", "dh_b3", "dh_b4", "dh_b5", "dh_b6", "dh_b7", "dh_b8", "dh_b9", "dh_b10",
  "dh_b20", "dh_b40", "dh_b80", "dh_b160", "dh_b320", "dh_b640", "dh_b1280",
  "dh_top10", "dh_top1K", "dh_ds_top10", "dh_ds_top1K", "dh_dt_top10", "dh_dt_top1K",
  "ph_b1", "ph_b2", "ph_b3", "ph_b4", "ph_b5", "ph_b6", "ph_b7", "ph_b8", "ph_b9", "ph_b10",
  "ph_b20", "ph_b40", "ph_b80", "ph_b160", "ph_b320", "ph_b640", "ph_b1280", "ph_top10", "ph_top1K",
  "ph_tf_top10", "ph_tf_top1K", "ph_tdf_top10", "ph_tdf_top1K"
)

val new_features = Array(
  "termID", "docID" ,
  "features0","features1","features2","features3","features4","features5","features6","features7","features8","features9",
  "features10","features11","features12","features13","features14","features15","features16","features17","features18","features19",
  "features20","features21","features22","features23","features24","features25","features26","features27","features28","features29",
  "features30","features31","features32","features33","features34","features35","features36","features37","features38","features39",
  "features40","features41","features42","features43","features44","features45","features46","features47","features48","features49",
  "features50","features51","features52","features53","features54","features55")


val model = new EasyPredictModelWrapper(MojoModel.load(modelName))

val toDouble = udf[Double,Int](_.toDouble)

val df = spark.read.parquet(fileName)
  .select("termID",features:_*)
  .toDF(new_features:_*)
  .withColumn("features0",toDouble($"features0"))
  .withColumn("features1",toDouble($"features1"))
  .withColumn("features2",toDouble($"features2"))
  .withColumn("features3",toDouble($"features3"))
  .withColumn("features4",toDouble($"features4"))
  .withColumn("features5",toDouble($"features5"))
  .withColumn("features6",toDouble($"features6"))
  .withColumn("features7",toDouble($"features7"))
  .withColumn("features8",toDouble($"features8"))
  .withColumn("features9",toDouble($"features9"))
  .withColumn("features10",toDouble($"features10"))
  .withColumn("features11",toDouble($"features11"))
  .withColumn("features12",toDouble($"features12"))
  .withColumn("features13",toDouble($"features13"))
  .withColumn("features14",toDouble($"features14"))
  .withColumn("features15",toDouble($"features15"))
  .withColumn("features16",toDouble($"features16"))
  .withColumn("features17",toDouble($"features17"))
  .withColumn("features18",toDouble($"features18"))
  .withColumn("features19",toDouble($"features19"))
  .withColumn("features20",toDouble($"features20"))
  .withColumn("features21",toDouble($"features21"))
  .withColumn("features22",toDouble($"features22"))
  .withColumn("features23",toDouble($"features23"))
  .withColumn("features24",toDouble($"features24"))
  .withColumn("features25",toDouble($"features25"))
  .withColumn("features26",toDouble($"features26"))
  .withColumn("features27",toDouble($"features27"))
  .withColumn("features28",toDouble($"features28"))
  .withColumn("features29",toDouble($"features29"))
  .withColumn("features30",toDouble($"features30"))
  .withColumn("features31",toDouble($"features31"))
  .withColumn("features32",toDouble($"features32"))
  .withColumn("features33",toDouble($"features33"))
  .withColumn("features34",toDouble($"features34"))
  .withColumn("features35",toDouble($"features35"))
  .withColumn("features36",toDouble($"features36"))
  .withColumn("features37",toDouble($"features37"))
  .withColumn("features38",toDouble($"features38"))
  .withColumn("features39",toDouble($"features39"))
  .withColumn("features40",toDouble($"features40"))
  .withColumn("features41",toDouble($"features41"))
  .withColumn("features42",toDouble($"features42"))
  .withColumn("features43",toDouble($"features43"))
  .withColumn("features44",toDouble($"features44"))
  .withColumn("features45",toDouble($"features45"))
  .withColumn("features46",toDouble($"features46"))
  .withColumn("features47",toDouble($"features47"))
  .withColumn("features48",toDouble($"features48"))
  .withColumn("features49",toDouble($"features49"))
  .withColumn("features50",toDouble($"features50"))
  .withColumn("features51",toDouble($"features51"))
  .withColumn("features52",toDouble($"features52"))
  .withColumn("features53",toDouble($"features53"))
  .withColumn("features54",toDouble($"features54"))
  .withColumn("features55",toDouble($"features55"))

import org.apache.spark.sql._

val d = df.map( r => {
  val row = new RowData
  row.put("features0", new java.lang.Double(r.getDouble(2+0)))
  row.put("features1", new java.lang.Double(r.getDouble(2+1)))
  row.put("features2", new java.lang.Double(r.getDouble(2+2)))
  row.put("features3", new java.lang.Double(r.getDouble(2+3)))
  row.put("features4", new java.lang.Double(r.getDouble(2+4)))
  row.put("features5", new java.lang.Double(r.getDouble(2+5)))
  row.put("features6", new java.lang.Double(r.getDouble(2+6)))
  row.put("features7", new java.lang.Double(r.getDouble(2+7)))
  row.put("features8", new java.lang.Double(r.getDouble(2+8)))
  row.put("features9", new java.lang.Double(r.getDouble(2+9)))
  row.put("features10", new java.lang.Double(r.getDouble(2+10)))
  row.put("features11", new java.lang.Double(r.getDouble(2+11)))
  row.put("features12", new java.lang.Double(r.getDouble(2+12)))
  row.put("features13", new java.lang.Double(r.getDouble(2+13)))
  row.put("features14", new java.lang.Double(r.getDouble(2+14)))
  row.put("features15", new java.lang.Double(r.getDouble(2+15)))
  row.put("features16", new java.lang.Double(r.getDouble(2+16)))
  row.put("features17", new java.lang.Double(r.getDouble(2+17)))
  row.put("features18", new java.lang.Double(r.getDouble(2+18)))
  row.put("features19", new java.lang.Double(r.getDouble(2+19)))
  row.put("features20", new java.lang.Double(r.getDouble(2+20)))
  row.put("features21", new java.lang.Double(r.getDouble(2+21)))
  row.put("features22", new java.lang.Double(r.getDouble(2+22)))
  row.put("features23", new java.lang.Double(r.getDouble(2+23)))
  row.put("features24", new java.lang.Double(r.getDouble(2+24)))
  row.put("features25", new java.lang.Double(r.getDouble(2+25)))
  row.put("features26", new java.lang.Double(r.getDouble(2+26)))
  row.put("features27", new java.lang.Double(r.getDouble(2+27)))
  row.put("features28", new java.lang.Double(r.getDouble(2+28)))
  row.put("features29", new java.lang.Double(r.getDouble(2+29)))
  row.put("features30", new java.lang.Double(r.getDouble(2+30)))
  row.put("features31", new java.lang.Double(r.getDouble(2+31)))
  row.put("features32", new java.lang.Double(r.getDouble(2+32)))
  row.put("features33", new java.lang.Double(r.getDouble(2+33)))
  row.put("features34", new java.lang.Double(r.getDouble(2+34)))
  row.put("features35", new java.lang.Double(r.getDouble(2+35)))
  row.put("features36", new java.lang.Double(r.getDouble(2+36)))
  row.put("features37", new java.lang.Double(r.getDouble(2+37)))
  row.put("features38", new java.lang.Double(r.getDouble(2+38)))
  row.put("features39", new java.lang.Double(r.getDouble(2+39)))
  row.put("features40", new java.lang.Double(r.getDouble(2+40)))
  row.put("features41", new java.lang.Double(r.getDouble(2+41)))
  row.put("features42", new java.lang.Double(r.getDouble(2+42)))
  row.put("features43", new java.lang.Double(r.getDouble(2+43)))
  row.put("features44", new java.lang.Double(r.getDouble(2+44)))
  row.put("features45", new java.lang.Double(r.getDouble(2+45)))
  row.put("features46", new java.lang.Double(r.getDouble(2+46)))
  row.put("features47", new java.lang.Double(r.getDouble(2+47)))
  row.put("features48", new java.lang.Double(r.getDouble(2+48)))
  row.put("features49", new java.lang.Double(r.getDouble(2+49)))
  row.put("features50", new java.lang.Double(r.getDouble(2+50)))
  row.put("features51", new java.lang.Double(r.getDouble(2+51)))
  row.put("features52", new java.lang.Double(r.getDouble(2+52)))
  row.put("features53", new java.lang.Double(r.getDouble(2+53)))
  row.put("features54", new java.lang.Double(r.getDouble(2+54)))
  row.put("features55", new java.lang.Double(r.getDouble(2+55)))
  (r.getInt(0), r.getInt(1),model.predictRegression(row).value)
}).toDF("termID","docID","predict")

d.write.parquet(fileName+".predict")

