package psug.hands.on.exercise08

import org.apache.spark.rdd.RDD
import psug.hands.on.exercise05.{City, DataSaver}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, DataFrame, SQLContext}
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithSGD}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint

/**
 * - Train a Linear Regression model using training_cities.json
 * - Predict for each city in test_cities.json if it has more than 5000 inhabitants
 * - Save result in a labeled_cities.json file
 *
 * input file 1 : data/training_cities.json
 * input file 2 : data/test_cities.json
 * output file : data/labeled_cities.json
 *
 * command : sbt "run-main psug.hands.on.exercise08.MachineLearning"
 */
object MachineLearning extends App with DataSaver {

  val trainingInputFile = "data/training_cities.json"
  val testInputFile = "data/test_cities.json"
  val outputFile = "data/labeled_cities.json"

  val sparkConf = new SparkConf()
    .setAppName("hands-on")
    .setMaster("local[8]")
  val sparkContext = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sparkContext)

  init()

  def getFeaturesVector(row: Row): Vector = {
    val rawFeatures = row.getAs[List[Double]]("features")
    Vectors.dense(rawFeatures.toArray)
  }


  val labelPoints: RDD[LabeledPoint] = sqlContext.read.json(trainingInputFile)
    .map(r => LabeledPoint(r.getAs[Double]("category"), getFeaturesVector(r)))
    .cache()

  val numIterations: Int = 1000
  val stepSize: Double = 1000
  val model: LogisticRegressionModel = LogisticRegressionWithSGD.train(labelPoints, numIterations, stepSize)

  import sqlContext.implicits._

  val labeledCities: RDD[String] = sqlContext.read.json(testInputFile)
    .map(r => {
      val prediction = model.predict(getFeaturesVector(r))
      (r.getAs[String]("name"), r.getAs[Double]("category"), prediction)
    })
    .toDF("name", "category", "prediction")
    .toJSON

  labeledCities.saveAsTextFile(temporaryFile + "/1")
  merge(temporaryFile + "/1", outputFile)

  sparkContext.stop()
}
