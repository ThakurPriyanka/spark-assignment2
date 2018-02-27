package edu.knoldus.util

import org.apache.spark.sql.{DataFrame, SparkSession}

object FootBallData {
  def getFootBallData(spark: SparkSession): DataFrame = {
    val data = spark.read.option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/D1.csv")
    data
  }
}
