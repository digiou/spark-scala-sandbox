package xyz.digiou.spark

import scala.io.Source._
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object SortByAmountSpent {
  
  def getFields(x: String): (Int, Float) = {
    // Only interested in ID and amount of TX
    val fields = x.split(",")
    (fields(0).toInt, fields(2).toFloat)
  }
  
  /** main method of solution */
  def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "SortByAmountSpent")
    
    val lines = sc.textFile("../data/customer-orders.csv")
    val tuples = lines.map(getFields)
    val reducedTuples = tuples.reduceByKey( (x, y) => x + y )
    val sortedTuples = reducedTuples.map( x => (x._2, x._1) ).sortByKey()
    
    val results = sortedTuples.collect
    
    for (result <- results) {
      println(result._2 + ", " + result._1)
    }
  }
}