package edu.knoldus.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkConfigrationFile {
  def getSparkSession(): SparkSession = {
    val conf = new SparkConf().setAppName("practice").setMaster("local[*]")

    val spark = SparkSession.builder().config(conf).getOrCreate()
    spark
  }
}
