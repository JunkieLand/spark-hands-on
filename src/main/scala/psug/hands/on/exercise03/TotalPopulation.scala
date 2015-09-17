package psug.hands.on.exercise03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, DataFrame, SQLContext}

/**
 * Determine the total population in France
 *
 * input file : data/demographie_par_commune.json
 *
 * command : sbt "run-main psug.hands.on.exercise03.TotalPopulation"
 */
object TotalPopulation extends App {

  val inputFile = "data/demographie_par_commune.json"

  val sparkConf = new SparkConf()
    .setMaster("local")
    .setAppName("hands-on")

  val context = new SparkContext(sparkConf)
  val sqlContext: SQLContext = new SQLContext(context)
  val json: DataFrame = sqlContext.read.json(inputFile)

  import org.apache.spark.sql.functions._

  val population:Long = json
    .select("Population")
    .agg(sum("Population"))
    .head()
    .getLong(0) // TODO extract total population in France in 2010

  println("La France compte " + population + " habitants")

  context.stop()

}
