package psug.hands.on.exercise01

import java.lang.Math.pow

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Compute the sum of squares of numbers that are under 100 and that are not prime.
 *
 * For instance, the same sum but for numbers that are under 10 is 1^2 + 4^2 + 6^2 + 8^2 + 9^2 + 10^2 = 298
 *
 * command : sbt "run-main psug.hands.on.exercise01.SumOfSquaresOfNonPrimeNumbers"
 *
 */
object SumOfSquaresOfNonPrimeNumbers extends App {

  val startingNumbersList = 1 to 75
  val endingNumbersList = 50 to 125
  val primeNumbersList = List(2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97)

  val sparkConf = new SparkConf()
    .setMaster("local")
    .setAppName("hands-on")

  val context = new SparkContext(sparkConf)

  val startingNbRDD: RDD[Int] = context.makeRDD(startingNumbersList)
  val endingNbRDD: RDD[Int] = context.makeRDD(endingNumbersList)
  val primeNbRDD: RDD[Int] = context.makeRDD(primeNumbersList)

  val sumOfSquaresOfNonPrimeNumbers = startingNbRDD
    .union(endingNbRDD)
    .filter(_ <= 100)
    .distinct()
    .subtract(primeNbRDD)
    .map(a => a*a)
    .sum()
//    .fold(0)((acc, i) => acc + pow(i, 2).toInt) // TODO compute the sum of squares of numbers under 100 that are not prime

  println(s"The sum of square of numbers that are not prime and are under 100 is $sumOfSquaresOfNonPrimeNumbers")

  context.stop()
}
