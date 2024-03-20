//TASK 2
package edu.ucr.cs.cs167.aheck004

import edu.ucr.cs.bdlab.beast.geolite.{Feature, IFeature}
import org.apache.spark.SparkConf
import org.apache.spark.beast.SparkSQLRegistration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.Map

import org.apache.spark.sql.types._

object BeastScala {
  def main(args: Array[String]): Unit = {
    // Initialize Spark context

    val conf = new SparkConf().setAppName("Beast Example")
    // Set Spark master to local if not already set
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")

    val spark: SparkSession.Builder = SparkSession.builder().config(conf)

    val sparkSession: SparkSession = spark.getOrCreate()
    // Change scala from 4.5.6 to 4.6.1? to 4.4.0?
    val sparkContext = sparkSession.sparkContext
    SparkSQLRegistration.registerUDT
    SparkSQLRegistration.registerUDF(sparkSession)

    val operation: String = args(0)
    val inputFile: String = args(1)
    try {
      // Import Beast features
      import edu.ucr.cs.bdlab.beast._
      val t1 = System.nanoTime()
      var validOperation = true

      operation match {

        case "choropleth-map" =>
          val start: String = args(2)
          val end: String = args(3)
          val outputFile: String = args(4)
          // Writes a Shapefile that contains the count of the given keyword by county
          // Load the converted dataset in Parquet format and create a view with that name to be able
          // to run SQL queries.
          val wildfireDF = sparkSession.read.parquet(inputFile)
          wildfireDF.createOrReplaceTempView("wildfires")

          wildfireDF.show()

          // Run SQL query to compute the total fire intensity for each county over the given time range.
          val fireIntensityByCountyDF = sparkSession.sql(
            s"""
               |SELECT County, SUM(frp) AS fire_intensity
               |FROM wildfires
               |WHERE to_date(acq_date, 'yyyy-MM-dd') BETWEEN to_date('$start', 'MM/dd/yyyy') AND to_date('$end', 'MM/dd/yyyy')
               |GROUP BY County
               |""".stripMargin)

          fireIntensityByCountyDF.show()

          // Put the result of the query in a view named fire_intensity.
          fireIntensityByCountyDF.createOrReplaceTempView("fire_intensity")

          // Load the county dataset as a DataFrame using Beast.
          val countiesDF = sparkSession.read.format("shapefile").load("tl_2018_us_county.zip")
          countiesDF.createOrReplaceTempView("counties")

          // Run SQL query to join the fire intensity results with county dataset to get county name and geometry.
          val choroplethMapDF = sparkSession.sql(
            """
              |SELECT c.GEOID as County, c.NAME, c.geometry as g, f.fire_intensity
              |FROM fire_intensity f
              |JOIN counties c ON f.County = c.GEOID
              |""".stripMargin)


          // Save the result in a standard format
          //choroplethMapDF.coalesce(1).write.format("shapefile").save(outputFile)
          choroplethMapDF.toSpatialRDD.coalesce(1).saveAsShapefile(outputFile)

          // Show the schema and some sample data
          choroplethMapDF.printSchema()
          choroplethMapDF.show()


        case _ => validOperation = false
      }
      val t2 = System.nanoTime()
      if (validOperation)
        println(s"Operation '$operation' on file '$inputFile' took ${(t2 - t1) * 1E-9} seconds")
      else
        Console.err.println(s"Invalid operation '$operation'")
    } finally {
      sparkSession.stop()
    }
  }
}