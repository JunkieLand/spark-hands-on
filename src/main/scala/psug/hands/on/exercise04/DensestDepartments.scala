package psug.hands.on.exercise04

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Column, DataFrame, SQLContext}

/**
 * Which are the densest departments ?
 *
 * input file 1 : data/departements.txt
 * input file 2 : data/demographie_par_commune.json
 *
 * command : sbt "run-main psug.hands.on.exercise04.DensestDepartments"
 */
object DensestDepartments extends App {

  val departmentsFile = "data/departements.txt"
  val inputFile = "data/demographie_par_commune.json"

  val sparkConf = new SparkConf()
    .setMaster("local[8]")
    .setAppName("hands-on")

  val context = new SparkContext(sparkConf)
  val sqlContext: SQLContext = new SQLContext(context)
  import org.apache.spark.sql.functions._

  val nbAndDep: RDD[(String, String)] = context.textFile(departmentsFile)
    .map(l => l.split(','))
    .map(a => (a(1), a(0)))

  val departement = "Departement"
  val population = "Population"
  val superficie = "Superficie"

  val densestDepartments:Iterable[String] = sqlContext.read.json(inputFile)
    .select(departement, population, superficie)
    .groupBy(departement)
    .agg(sum(population).as("totalPopulation"), sum(superficie).as("totalSuperficie"))
    .map(row => (row.getString(0), row.getLong(1) / row.getLong(2)))
    .join(nbAndDep)
    .values
    .sortBy(t => t._1, false)
    .values
    .take(10) // TODO Create an iterable ("densestDepartment","secondDensestDepartment",...)

  println("Les départements les plus densément peuplés sont " + densestDepartments.mkString(", "))

  context.stop()

}
