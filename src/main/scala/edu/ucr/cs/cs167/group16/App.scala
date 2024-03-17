package edu.ucr.cs.cs167.group16

import org.apache.spark.sql.functions.{avg, month, sum, year}
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{VectorAssembler, StandardScaler}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.evaluation.RegressionEvaluator

object App {

  def main(args : Array[String]) {
    println( "Project C: Group 16" )

    val conf = new org.apache.spark.SparkConf
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")
    println(s"Using Spark master '${conf.get("spark.master")}'")

    val spark = SparkSession
      .builder()
      .appName("CS167 Group C 16")
      .config(conf)
      .getOrCreate()

    val t1 = System.nanoTime

    try {
      // Task 1

      // Task 2

      // Task 3


      // Task 4
      var inputfile : String = "sampleData/wildfiredb_sample.parquet"
      var df : DataFrame = spark.read.parquet(inputfile)

      var processedDF: DataFrame = df.withColumn("year", year(df("acq_date")))
        .withColumn("month", month(df("acq_date")))

      val groupedDF = processedDF.groupBy("county", "year", "month")
      val aggregatedDF = groupedDF.agg(
        sum("frp").alias("fire_intensity"), // No casting, we'll round later
        avg("ELEV_mean").alias("avg_ELEV_mean"),
        avg("SLP_mean").alias("avg_SLP_mean"),
        avg("EVT_mean").alias("avg_EVT_mean"),
        avg("EVH_mean").alias("avg_EVH_mean"),
        avg("CH_mean").alias("avg_CH_mean"),
        avg("TEMP_ave").alias("avg_TEMP_ave"),
        avg("TEMP_min").alias("avg_TEMP_min"),
        avg("TEMP_max").alias("avg_TEMP_max")
      )

      // Round the fire_intensity column to the nearest integer
      // Have to round because I get a label decimal error if I don't do this step
      val roundedDF = aggregatedDF.withColumn("fire_intensity", aggregatedDF("fire_intensity").cast("int"))

      val inputFeatures = Array("avg_ELEV_mean", "avg_SLP_mean", "avg_EVT_mean", "avg_EVH_mean", "avg_CH_mean", "avg_TEMP_ave", "avg_TEMP_min", "avg_TEMP_max")
      val assembler = new VectorAssembler()
        .setInputCols(inputFeatures)
        .setOutputCol("unscaled_features")

      val scaler = new StandardScaler()
        .setInputCol("unscaled_features")
        .setOutputCol("features")
        .setWithStd(true)
        .setWithMean(true)

      val lr = new LogisticRegression()
        .setLabelCol("fire_intensity")
        .setFeaturesCol("features")
        .setMaxIter(10)
        .setRegParam(0.01)

      val pipeline = new Pipeline()
        .setStages(Array(assembler, scaler, lr))

      val model = pipeline.fit(roundedDF)

      val predictionsDF = model.transform(roundedDF)

      // Rename columns to match desired format
      val renamedColumnsDF = predictionsDF.selectExpr("avg_ELEV_mean as ELEV_mean",
        "avg_SLP_mean as SLP_mean",
        "avg_EVT_mean as EVT_mean",
        "avg_EVH_mean as EVH_mean",
        "avg_CH_mean as CH_mean",
        "avg_TEMP_ave as TEMP_ave",
        "avg_TEMP_min as TEMP_min",
        "avg_TEMP_max as TEMP_max",
        "fire_intensity",
        "prediction")

      renamedColumnsDF.show()

      // Compute total time
      val t2 = System.nanoTime
      val totalTime = (t2 - t1) / 1e9
      println(s"Total time: $totalTime seconds")

      // Compute RMSE
      val evaluator = new RegressionEvaluator()
        .setLabelCol("fire_intensity")
        .setPredictionCol("prediction")
        .setMetricName("rmse")

      val rmse = evaluator.evaluate(predictionsDF)
      println(s"RMSE: $rmse")

    } finally {
      spark.stop()
    }
  }
}
