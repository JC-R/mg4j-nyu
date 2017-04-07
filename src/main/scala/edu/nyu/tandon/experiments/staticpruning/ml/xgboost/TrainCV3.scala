package edu.nyu.tandon.experiments.staticpruning.ml.xgboost

import ml.dmlc.xgboost4j.scala.spark.XGBoostEstimator
import org.apache.spark.SparkContext
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.tuning._
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable

/**
  * Created by juan on 12/28/16.
  */
object TrainCV3 {

  private def crossValidation(
       label: String,
       xgboostParam: Map[String, Any],
       trainingData: Dataset[_]): TrainValidationSplitModel = {

    val xgbEstimator = new XGBoostEstimator(xgboostParam)
      .setFeaturesCol("features")
      .setLabelCol(label)

    val paramGrid = new ParamGridBuilder()
      .addGrid(xgbEstimator.round, Array(100, 400))
      .addGrid(xgbEstimator.eta, Array(0.02, 0.05))
      .build()

    val tv = new TrainValidationSplit()
      .setEstimator(xgbEstimator)
      .setEvaluator(new RegressionEvaluator().setLabelCol(label))
      .setEstimatorParamMaps(paramGrid)
      .setTrainRatio(0.75)  // Use 3+ in practice

    tv.fit(trainingData)

  }

  private def train( implicit sc : SparkContext,
         key: String,
        value : Array[String],
        params: Map[String,Any],
        trainingData: Dataset[_],
        label :String,
        prefix: String) =
  {
    println("Generating: " + label + " - " + key)

    val stage = new VectorAssembler()
      .setInputCols(value)
      .setOutputCol("features")

    val train = stage.transform(trainingData)

    val xgbEstimator = new XGBoostEstimator(params.toMap)
      .setFeaturesCol("features")

    println("Xgboost training ...")
    xgbEstimator.setLabelCol(label).train(train).saveModelAsHadoopFile(prefix + "." + key + "." + label + ".model")

  }

  def main(args: Array[String]): Unit = {

    if (args.length < 7) {
      System.err.println("Usage: Train <features> <top10> <top1k> <prefix> <nrounds> <maxDepth> <nthread>")
      System.exit(1)
    }

    val raw_data = args(0)
    val labels_10 = args(1)
    val labels_1k = args(2)
    val prefix = args(3)

    val nrounds = args(4).toInt
    val maxDepth = args(5).toInt
    val nthreads = args(6).toInt

    val spark = SparkSession
      .builder()
      .appName("Train")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    implicit val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    // feature groups
    //---------------
    val docFeatures = Array("d_docSize", "d_docTerms", "d_xdoc", "d_doc_s_t", "d_xdoc_s", "d_xdoc_t")

    val termFeatures = Array("p_tfreq", "p_tdfreq", "p_bm25","pt_pt")

    val dhFeatures = Array("dh_b1", "dh_b2", "dh_b3", "dh_b4", "dh_b5", "dh_b6", "dh_b7", "dh_b8", "dh_b9", "dh_b10",
      "dh_b20", "dh_b40", "dh_b80", "dh_b160", "dh_b320", "dh_b640", "dh_b1280",
      "dh_top10", "dh_top1K", "dh_ds_top10", "dh_ds_top1K", "dh_dt_top10", "dh_dt_top1K")

    val phFeatures = Array("ph_b1", "ph_b2", "ph_b3", "ph_b4", "ph_b5", "ph_b6", "ph_b7", "ph_b8", "ph_b9", "ph_b10",
      "ph_b20", "ph_b40", "ph_b80", "ph_b160", "ph_b320", "ph_b640", "ph_b1280",
      "ph_top10", "ph_top1K", "ph_tf_top10", "ph_tf_top1K", "ph_tdf_top10", "ph_tdf_top1K")

    val raw_features = termFeatures ++ docFeatures ++ dhFeatures ++ phFeatures
    val intrinsic_features = docFeatures ++ termFeatures
    val docbased_features = docFeatures ++ dhFeatures
    val termbased_features = termFeatures ++ phFeatures

    val featureGroups = Map(
      "doc" -> docFeatures,
      "term" -> termFeatures,
      "ph" -> phFeatures,
      "dh" -> dhFeatures,
      "termbased" -> termbased_features,
      "intrinsic" -> intrinsic_features,
      "docbased" -> docbased_features
    )

    val params = new mutable.HashMap[String, Any]()
    params += "eta" -> 0.05
    params += "max_depth" -> maxDepth
    params += "ntreelimit" -> 1000
    params += "objective" -> "reg:linear"
    params += "subsample" -> 0.8
    params += "num_round" -> nrounds
    params += "silent" -> 0
    params += "useExternalMemory" -> true

    val t10 = spark.read
      .format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
      .load(labels_10)
      .na.fill(0)
    println(s"top10 sample: ${t10.count}")

    val t1k = spark.read
      .format("org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")
      .load(labels_1k)
        .sample(false, 0.35)
      .na.fill(0)
    println(s"top1k sample: ${t1k.count}")

    train(sc,"all",raw_features,params.toMap,t1k,"top1K",prefix)
    featureGroups.foreach { case (key, value) =>
      train(sc,key, value, params.toMap, t1k, "top1K", prefix)
      train(sc,key, value, params.toMap, t10, "top10", prefix)
    }

    train(sc,"all",raw_features,params.toMap,t1k,"top10",prefix)
    featureGroups.foreach { case (key, value) =>
      train(sc,key, value, params.toMap, t10, "top10", prefix)
    }

    spark.stop()
  }

}