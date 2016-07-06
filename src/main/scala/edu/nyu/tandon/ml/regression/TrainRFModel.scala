package edu.nyu.tandon.ml.regression

import java.io.{File, FileWriter, PrintWriter}

import edu.nyu.tandon.ml.features.FeatureJoin
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

/**
  * @author michal.siedlaczek@nyu.edu
  */
object TrainRFModel {

  val FeaturesCol: String = "features"
  val LabelCol: String = "label"
  val IdCol: String = "id"

  def featureAssembler(df: DataFrame): VectorAssembler =
    if (df.schema.fieldNames.contains(FeaturesCol))
      throw new IllegalStateException(s"Can't assemble features: column '${FeaturesCol}' already exists")
    else new VectorAssembler()
      .setInputCols(df.columns.filter(x => x != LabelCol && x != IdCol))
      .setOutputCol(FeaturesCol)

  def regressor(numTrees: Int, maxBins: Int, maxDepth: Int): RandomForestRegressor = new RandomForestRegressor()
    .setNumTrees(numTrees)
    .setMaxBins(maxBins)
    .setMaxDepth(maxDepth)
    .setLabelCol(LabelCol)
    .setFeaturesCol(FeaturesCol)

  def model(trainingData: DataFrame, numTrees: Int, maxBins: Int, maxDepth: Int, numFolds: Int): CrossValidatorModel = {
    val pipeline = new Pipeline().setStages(Array(
      featureAssembler(trainingData),
      regressor(numTrees, maxBins, maxDepth)
    ))

    val crossValidator = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new RegressionEvaluator())
      .setEstimatorParamMaps(Array(pipeline.extractParamMap()))
      .setNumFolds(numFolds)

    crossValidator.fit(trainingData)
  }

  def main(args: Array[String]): Unit = {

    case class Config(dataFiles: Seq[File] = List(),
                      outputFile: File = null,
                      numFolds: Int = 10,
                      numTrees: Int = 50,
                      maxBins: Int = 20,
                      maxDepth: Int = 15)

    val default = new Config()
    val parser = new OptionParser[Config](this.getClass.getSimpleName) {

      opt[Seq[File]]('i', "input")
        .action((x, c) => c.copy(dataFiles = x))
        .text("file(s) containing training data")
        .required()

      opt[Int]('F', "num-folds")
        .action((x, c) => c.copy(numFolds = x))
        .text(s"the number of cross-validation folds (default: ${default.numFolds})")

      opt[Int]('T', "num-trees")
        .action((x, c) => c.copy(numFolds = x))
        .text(s"the number of trees in RF (default: ${default.numTrees})")

      opt[Int]('B', "max-bins")
        .action((x, c) => c.copy(maxBins = x))
        .text(s"the maximum number of bins in RF (default: ${default.maxBins})")

      opt[Int]('D', "max-depth")
        .action((x, c) => c.copy(maxDepth = x))
        .text(s"the maximum depth of trees in RF (default: ${default.maxDepth})")

      opt[File]('o', "output")
        .action((x, c) => c.copy(outputFile = x))
        .text("the output file for the trained model")
        .required()

    }

    parser.parse(args, Config()) match {
      case None =>
      case Some(config) =>

        val sparkContext = new SparkContext(new SparkConf().setAppName("Train Model").setMaster("local[*]"))
        val sqlContext = new SQLContext(sparkContext)

        val Array(trainingData, testData) = FeatureJoin.join(sqlContext)(config.dataFiles)
          .randomSplit(Array(0.7, 0.3))

        val m = model(trainingData, config.numTrees, config.maxBins, config.maxDepth, config.numFolds)

        val testPredictions = m.transform(testData)
        val eval = new RegressionEvaluator().evaluate(testPredictions)
        new FileWriter(config.outputFile.getAbsolutePath + ".eval")
          .append(eval.toString)
          .close()

        m.save(config.outputFile.getAbsolutePath)

    }

  }

}
