package psug.hands.on.exercise06

import org.apache.spark.rdd.RDD
import psug.hands.on.exercise05.{City, DataSaver}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, DataFrame, SQLContext}

/**
 * Normalize features retrieved in previous exercice 05 so a Machine Learning algorithm can swallow them
 *
 * input file : data/cities.json
 *
 * command : sbt "run-main psug.hands.on.exercise06.Normalization"
 *
 */
object Normalization extends App with DataSaver with AggregateFunctions with Normalizer {

  val inputFile = "data/cities.json"
  val outputFile = "data/normalized_cities.json"

  init()

  val sparkConf = new SparkConf()
    .setAppName("hands-on")
    .setMaster("local[8]")
  val sparkContext = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sparkContext)

  val df: DataFrame = sqlContext.read.json(inputFile).cache()

  val extremes: List[Extremes] = df.rdd.aggregate(initValue(5))((acc, r) => reduce(acc, r.getAs[List[Double]]("features")), merge)

  import sqlContext.implicits._

  // TODO generate JSON strings, each representing a City object whose features have been normalized
  val normalizedCities: RDD[String] = df
    .map(r => normalize(extremes)(City(r)))
    .toDF()
    .toJSON

  normalizedCities.saveAsTextFile(temporaryFile + "/1")
  merge(temporaryFile + "/1", outputFile)

  println("Some lines of data/normalized_cities.json : ")
  normalizedCities.take(10).foreach(println)

  sparkContext.stop()
}
