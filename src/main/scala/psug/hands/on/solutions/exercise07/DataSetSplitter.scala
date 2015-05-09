package psug.hands.on.solutions.exercise07

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import psug.hands.on.exercise05.DataSaver
import psug.hands.on.solutions.SparkContextInitiator

object DataSetSplitter extends App with DataSaver with SparkContextInitiator {

  val inputFile = "data/normalized_cities.json"
  val trainingCitiesFile = "data/training_cities.json"
  val testCitiesFile = "data/test_cities.json"

  init()

  val sparkContext = initContext("machineLearningMLLib")
  val sqlContext = new SQLContext(sparkContext)

  val normalizedCities = sqlContext.jsonFile(inputFile)
  normalizedCities.registerTempTable("dataset")

  val trainingData: DataFrame = normalizedCities.sample(false, 0.1).select("name", "category", "features")
  trainingData.registerTempTable("training")

  val testData = sqlContext.sql("SELECT name, category, features FROM dataset EXCEPT SELECT name, category, features FROM training")

  val trainingCities:RDD[String] = trainingData.toDF().toJSON
  val testCities:RDD[String] = testData.toDF().toJSON

  trainingCities.saveAsTextFile(temporaryFile + "/1")
  merge(temporaryFile + "/1", trainingCitiesFile)

  testCities.saveAsTextFile(temporaryFile + "/2")
  merge(temporaryFile + "/2", testCitiesFile)

  println("There are " + testCities.count() + " test cities and " + trainingCities.count() + " training cities")

  sparkContext.stop()

}
