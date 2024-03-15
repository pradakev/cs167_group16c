package edu.ucr.cs.cs167.group16
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.Map
import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.{LinearSVC, LinearSVCModel}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, StringIndexer, Tokenizer}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit, TrainValidationSplitModel}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
/**
 * @author ${Group 16 }
 */
object App {

  def main(args : Array[String]) {
    println( "Project C: Group 16" )

    val conf = new SparkConf
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")
    println(s"Using Spark master '${conf.get("spark.master")}'")

    val spark = SparkSession
      .builder()
      .appName("CS167 Group C 16")
      .config(conf)
      .getOrCreate()

    val t1 = System.nanoTime

    try{
      // Task #1

      // Task #2

      // Task #3

      // Task #4: Kevin
      var inputfile : String = "sampleData/wildfiredb_sample.parquet"
      var df : DataFrame = spark.read.parquet(inputfile)
      df.show()
    } finally {
      spark.stop
    }
  }
}
