package com.sparkwithscala

import org.apache.log4j._
import org.apache.spark._
/** 3 */
object RatingsCounter {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "RatingCounter")

    val lines = sc.textFile("data/ml-100k/u.data")
    // Split all values by given regex
    val ratings = lines.map(x=>x.split("\t")(2))

    // count same values and result as tuple
    val results = ratings.countByValue()

    // Sort result map of rating (rating,count) tuples
    val sortValue = results.toSeq.sortBy(_._1)

    sortValue.foreach(println)

    sc.stop()
  }
}
