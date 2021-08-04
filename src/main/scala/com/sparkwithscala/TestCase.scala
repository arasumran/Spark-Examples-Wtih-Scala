package com.sparkwithscala

import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{avg, col, lit, round, to_date}

import java.util.Date


object TestCase {
  
  case class Exhibit(PLAY_ID:String, SONG_ID:Int, CLIENT_ID:Int, PLAY_TS:String)

  /** Our main function where the action happens */
  def main(args: Array[String]) {
    
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Use new SparkSession interface in Spark 2.0
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    // Convert our csv file to a DataSet, using our Person case
    // class to infer the schema.
    import spark.implicits._
    val exhibit = spark.read
      .option("header","true")
      .option("sep", "\t")
      .option("inferSchema", "true")
      .csv("data/exhibitA-input.csv")
      .as[Exhibit]

    // There are lots of other ways to make a DataFrame.
    // For example, spark.read.json("json file path")
    // or sqlContext.table("Hive table name")
    
    println("Here is our inferred schema:")
    val dfWithDate = exhibit.withColumn("Date", to_date($"PLAY_TS", "MM/dd/yyyy HH:mm:ss"))
    val firstLayerDf=dfWithDate.drop("PLAY_TS")
      .filter(dfWithDate("Date") === lit("2016-10-08")).dropDuplicates("SONG_ID")
    firstLayerDf.show(100)
    val secondLayerDf  = firstLayerDf.select("SONG_ID","CLIENT_ID")
      .groupBy("CLIENT_ID").count().withColumnRenamed("count","DISTINCT_PLAY_COUNT")
    secondLayerDf.printSchema()

    // here is last result
    println("here is last result ")
    secondLayerDf.groupBy("DISTINCT_PLAY_COUNT").count().withColumnRenamed("count","CLIENT_COUNT").show(100)
    spark.stop()
  }
}