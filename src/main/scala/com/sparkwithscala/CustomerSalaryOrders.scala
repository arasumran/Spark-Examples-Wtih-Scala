package com.sparkwithscala

import org.apache.log4j._
import org.apache.spark._

/** Count up how many of each word appears in a book as simply as possible. */

/** 7  ************************************************************************** */
// MAP : transforms each element of an RDD into one new element

// FLAPMAP : can create many new elements from each one

/**  ************************************************************************** */
object CustomerSalaryOrders {
  def extractCustomerPriceList(lines : String): (Int,Float) = {
    val fields = lines.split(",")
    (fields(0).toInt,fields(2).toFloat)
  }
 
  def main(args: Array[String]) {
   
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]", "CustomerPricePairing")
    val input = sc.textFile("data/customer-orders.csv")
    val fields = input.map(extractCustomerPriceList)
    val mappedRdd = fields.reduceByKey( (x,y) => x + y )
    val forSorting = mappedRdd.map(x=> (x._2,x._1)).sortByKey()
    val results = forSorting.collect()

    for (result <- results) {
      val priceVal = result._1
      val customerId = result._2
      println(s"Number $customerId. customer total price : ${priceVal}$$")
    }

  }
  
}
