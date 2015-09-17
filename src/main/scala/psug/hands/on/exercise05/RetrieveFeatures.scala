package psug.hands.on.exercise05

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Save the following information in a JSON file :
 *
 * - City name
 * - If the city has more than 5000 inhabitants
 * - City density
 * - Percentage of executives
 * - Percentage of employees
 * - Percentage of workers
 * - Percentage of farmers
 *
 *
 * input file : data/demographie_par_commune.json
 * output file : data/cities.json
 *
 * command : sbt "run-main psug.hands.on.exercise05.RetrieveFeatures"
 *
 */
object RetrieveFeatures extends App  with DataSaver {

  val inputFile = "data/demographie_par_commune.json"
  val outputFile = "data/cities.json"

  init()

  val sparkConf = new SparkConf()
    .setAppName("hands-on")
    .setMaster("local")
  val context = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(context)

  // {"CodeInsee":"01008","Departement":"01","Commune":"Ambutrix","Population":729,"Superficie":5,"Population0-14":142,"Population15-29":138,"Population30-44":179,"Population45-59":163,"Population60-74":77,"Population75+":31,"Agriculteurs":0,"Artisans/Liberaux":4,"Cadresetprofessionssupérieurs":44,"ProfessionsIntermédiaires":142,"Employés":80,"Ouvriers":142,"Retraités":89,"SansActivité(15+)":75}

  val df: DataFrame = sqlContext.read.json(inputFile)

  import sqlContext.implicits._

  def buildStats(row: Row): City = {
    def per100(num: Long, denom: Long): Float = (num.toFloat / denom.toFloat) * 100

    val name = row.getAs[String]("Commune")
    val pop = row.getAs[Long]("Population")
    val superficy = row.getAs[Long]("Superficie")
    val executives = row.getAs[Long]("Cadresetprofessionssupérieurs")
    val employees = row.getAs[Long]("Employés")
    val workers = row.getAs[Long]("Ouvriers")
    val farmers = row.getAs[Long]("Agriculteurs")

    val hasMore5000 = if (pop > 5000) 1 else 0
    val density = pop.toDouble / superficy.toDouble

    City(name, hasMore5000, List(density, per100(executives, pop), per100(employees, pop), per100(workers, pop), per100(farmers, pop)))
  }

  // TODO generate cities in France and their characteristics (name, has more than 5000 inhabitants...) JSON Strings
  val cities:RDD[String] = df
    .filter(df("Population") >= 2000L)
    .map(buildStats)
    .toDF()
    .toJSON
    .cache()

  cities.saveAsTextFile(temporaryFile + "/1")
  merge(temporaryFile + "/1", outputFile)

  println("Some lines of data/cities.json : ")
  cities.take(10).foreach(println)
  
  context.stop()
}
