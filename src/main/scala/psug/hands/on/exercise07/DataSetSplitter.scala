package psug.hands.on.exercise07

import org.apache.spark.rdd.RDD
import psug.hands.on.exercise05.{City, DataSaver}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
 * Split cities in data/normalized_cities.json in two groups :
 * - Training Cities (around 1/10 of normalized cities, randomly chosen)
 * - Test Cities (the rest of the cities)
 *
 * those two sets will be saved in respectively data/training_cities.json and data/test_cities.json
 *
 * input file : data/normalized_cities.json
 * output file 1 : data/training_cities.json
 * output file 2 : data/test_cities.json
 *
 * command : sbt "run-main psug.hands.on.exercise07.DataSetSplitter"
 *
 */
object DataSetSplitter extends App with DataSaver {

  val inputFile = "data/normalized_cities.json"
  val trainingCitiesFile = "data/training_cities.json"
  val testCitiesFile = "data/test_cities.json"

  init()

  val sparkConf = new SparkConf()
    .setAppName("hands-on")
    .setMaster("local[8]")
  val sparkContext = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sparkContext)

  val df: DataFrame = sqlContext.read.json(inputFile)

  // Version 1
//  val allCities: RDD[String] = df.toJSON.cache()
//  val trainingCities: RDD[String] = allCities.sample(true, 0.1)
//  val testCities: RDD[String] = allCities.subtract(trainingCities)

  // Version 2
  df.registerTempTable("allCities")
  val trainingCitiesDF: DataFrame = df.sample(true, 0.1)
  trainingCitiesDF.registerTempTable("trainingCities")
  val trainingCities: RDD[String] = trainingCitiesDF.toJSON
  val testCities: RDD[String] = sqlContext
    .sql("SELECT allCities.* FROM allCities EXCEPT SELECT trainingCities.* FROM trainingCities")
    .toJSON

  trainingCities.saveAsTextFile(temporaryFile + "/1")
  merge(temporaryFile + "/1", trainingCitiesFile)
  testCities.saveAsTextFile(temporaryFile + "/2")
  merge(temporaryFile + "/2", testCitiesFile)

  println("There are " + testCities.count() + " test cities and " + trainingCities.count() + " training cities")

  sparkContext.stop()
}
