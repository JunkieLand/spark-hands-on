package psug.hands.on.solutions.exercise03

import org.apache.spark.sql.SQLContext
import psug.hands.on.solutions.SparkContextInitiator

/**
 * Determine the total population in France
 *
 * Déterminer la population totale de la France
 *
 * file : data/demographie_par_commune.json
 * fichier : data/demographie_par_commune.json
 *
 */
object TotalPopulation extends App with SparkContextInitiator {

  val inputFile = "data/demographie_par_commune.json"

  val sparkContext = initContext("totalPopulation")
  val sqlContext = new SQLContext(sparkContext)

  val input = sqlContext.jsonFile(inputFile)

  import org.apache.spark.sql.functions._
  val population = input.filter("Population > 0").agg(sum("Population")).first().getLong(0)

  println("La France compte " + population + " habitants")

  sparkContext.stop()

}

