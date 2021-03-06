package psug.hands.on.exercise02

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

  val departmentsByRiver: Iterable[(String, String)] = ??? // TODO create an iterable ("River", Iterable("department1","department2"...)), ordered by river's name

  departmentsByRiver.foreach(row => println("Les départements dont le nom contient " + row._1 + " sont " + row._2))

}
