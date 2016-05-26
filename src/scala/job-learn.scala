import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.regression.RandomForestRegressionModel
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.Row
import org.apache.spark.ml.tuning
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}
import org.apache.spark.ml.evaluation._
import org.apache.spark.ml.param.ParamMap

// learn
// -----
val sc = new org.apache.spark.SparkContext()
val sqlContext = new org.apache.spark.sql.sqlContext(sc)

sc.setLogLevel("ERROR")
//val work_dir = "/san_data/research/juanr/posthits/"
//val work_dir = "/home/juan/work/save/"
val work_dir = "/user/juanr/data/"

val model_name="top1k"
val data = sqlContext.read.parquet(work_dir+model_name+".ph.train.parquet").na.fill(0.0).map(row => { 
val d = (for (i<-1 to 55) yield row.getDouble(i)).toArray
(row.getDouble(0), Vectors.dense(d)) 
}).toDF("label","features")

val splits = data.randomSplit(Array(0.6, 0.4))
val (trainingData, testData) = (splits(0), splits(1))

val rf = new RandomForestRegressor().setNumTrees(50).setMaxBins(20).setMaxDepth(15)
val evaluator = new RegressionEvaluator()
val cv = new CrossValidator().
setEstimator(rf).
setEvaluator(evaluator).
setEstimatorParamMaps(Array(rf.extractParamMap))

val model = cv.fit(trainingData)
model.save(work_dir+model_name+"_drf.model")

//val sameModel = RandomForestModel.load(sc, "target/tmp/myRandomForestRegressionModel")

