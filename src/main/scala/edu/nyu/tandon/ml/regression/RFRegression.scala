package edu.nyu.tandon.ml.regression

import java.io.{File, FileWriter}

import edu.nyu.tandon.ml._
import edu.nyu.tandon.ml.features.FeatureJoin
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import scopt.OptionParser

/**
  * @author michal.siedlaczek@nyu.edu
  */
class RFRegression(val numTrees: Int,
                   val maxBins: Int,
                   val maxDepth: Int) {

  def excludedFromFeatures: Set[String] = Set(LabelCol, FeaturesCol, IdCol)

  def featureAssembler(df: DataFrame): VectorAssembler =
    if (df.schema.fieldNames.contains(FeaturesCol))
      throw new IllegalStateException(s"Can't assemble features: column '$FeaturesCol' already exists")
    else new VectorAssembler()
      .setInputCols(df.columns.filterNot(excludedFromFeatures.contains))
      .setOutputCol(FeaturesCol)

  def regressor(numTrees: Int, maxBins: Int, maxDepth: Int): RandomForestRegressor = new RandomForestRegressor()
    .setNumTrees(numTrees)
    .setMaxBins(maxBins)
    .setMaxDepth(maxDepth)
    .setLabelCol(LabelCol)
    .setFeaturesCol(FeaturesCol)

  def fit(trainingData: DataFrame, stages: Array[PipelineStage], numFolds: Int): PipelineModel = {
    //    val paramGrid = new ParamGridBuilder()
    //      .build()
    //    val crossValidator = new CrossValidator()
    //      .setEstimator(pipeline)
    //      .setEvaluator(new RegressionEvaluator())
    //      .setEstimatorParamMaps(paramGrid)
    //      .setNumFolds(numFolds)
    //
    //    crossValidator.fit(trainingData)
    new Pipeline()
      .setStages(stages)
      .fit(trainingData)
  }

  def stages(trainingData: DataFrame): Array[PipelineStage] =
    Array(
      featureAssembler(trainingData),
      regressor(numTrees, maxBins, maxDepth)
    )

  def model(trainingData: DataFrame, numFolds: Int): PipelineModel =
    fit(
      trainingData,
      stages(trainingData),
      numFolds
    )
}

object RFRegression {

  case class Config(dataFiles: Seq[File] = List(),
                    outputFile: File = null,
                    numFolds: Int = 10,
                    numTrees: Int = 50,
                    maxBins: Int = 20,
                    maxDepth: Int = 15,
                    labelCol: String = LabelCol)

  def main(args: Array[String]): Unit = {

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

      opt[String]('L', "label-col")
        .action((x, c) => c.copy(labelCol = x))
        .text(s"the label column name (default: ${default.labelCol})")

      opt[File]('o', "output")
        .action((x, c) => c.copy(outputFile = x))
        .text("the output file for the trained model")
        .required()

    }

    parser.parse(args, Config()) match {
      case None =>
      case Some(config) =>

        val sparkContext = new SparkContext(new SparkConf().setAppName("Train Model").setMaster("local[1]"))
        val sqlContext = new SQLContext(sparkContext)
        val r = new RFRegression(config.numTrees, config.maxBins, config.maxDepth)

        val Array(trainingData, testData) = FeatureJoin.join(sqlContext)(config.dataFiles)
            .randomSplit(Array(0.7, 0.3))

        val m = r.model(trainingData, config.numFolds)

        val testPredictions = m.transform(testData)
        val eval = new RegressionEvaluator().evaluate(testPredictions)
        new FileWriter(config.outputFile.getAbsolutePath + ".eval")
          .append(eval.toString)
          .close()

        m.write.save(config.outputFile.getAbsolutePath)

    }

  }

}
