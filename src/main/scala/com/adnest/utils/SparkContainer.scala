package com.adnest.utils

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object SparkContainer {

  val spark : SparkSession = SparkSession.builder()
    .appName("adNestAnalytics")
    .master("local[*]")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext

  sc.setLogLevel("INFO")


}
