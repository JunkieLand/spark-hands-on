package psug.hands.on.exercise02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Which are the departments whose name contains Seine ? And Loire ? And Garonne ? And Rhône ?
 *
 * input file : data/departements.txt
 *
 * command : sbt "run-main psug.hands.on.exercise01.DepartmentsCounter"
 */
object DepartmentsByRiver extends App with RiversMatcher {

  val inputFile = "data/departements.txt"

  val sparkConf = new SparkConf()
    .setMaster("local")
    .setAppName("hands-on")

  val context = new SparkContext(sparkConf)

  val file: RDD[String] = context.textFile(inputFile)

  val collect: Array[(String, Iterable[String])] = file
    .map(line => line.split(',')(0))
    .flatMap(department => label(department))
    .groupByKey()
    .sortBy(tuple => tuple._1, true)
    .collect()

  val departmentsByRiver: Iterable[(String, Iterable[String])] = collect // TODO create an iterable ("River", Iterable("department1","department2"...)), ordered by river's name

  departmentsByRiver.foreach(row => println("Les départements dont le nom contient " + row._1 + " sont " + row._2))

  context.stop()

}
