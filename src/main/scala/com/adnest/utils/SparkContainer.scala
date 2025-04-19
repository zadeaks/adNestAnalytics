package com.adnest.utils

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object SparkContainer extends LogMaster {

  def initializeSparkSession(env: String): SparkSession = {

    val spark: SparkSession = SparkSession.builder()
      .appName("adNestAnalytics")
      .master("local[*]")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext

    logInfo(s"Spark Session for Application - ${sc.appName} with ID - ${sc.applicationId} is Created")

    sc.setLogLevel("INFO")

    spark

  }

}
