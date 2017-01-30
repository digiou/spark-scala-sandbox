package xyz.digiou.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

// Find the day with the most Percipitation
object MaxPercipitation {
  
  def parseLine(line:String)= {
    val fields = line.split(",")
    val day = fields(1)
    val entryType = fields(2)
    val percipitation = fields(3).toInt
    (day, entryType, percipitation)
  }
  
  /** main function */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MaxPercipitation")
    
    val lines = sc.textFile("../data/1800.csv")
    val parsedLines = lines.map(parseLine)
    val percipitations = parsedLines.filter(x => x._2 == "PRCP")
    val dayPercpTuples = percipitations.map(x => (x._1, x._3.toInt))
    val maxPercpDay = dayPercpTuples.max()(new Ordering[Tuple2[String, Int]]() {
      override def compare(x: (String, Int), y: (String, Int)): Int =
        Ordering[Int].compare(x._2, y._2)
    })
    println(maxPercpDay)
  }
}