package com.adnest

import com.adnest.core.builder.AdvertisementAnalyticsBuilder
import com.adnest.core.storageHandler.StorageHandler
import com.adnest.core.dataLoader.DataFrameLoader
import com.adnest.utils.{LogMaster, SparkContainer}
import org.apache.spark.sql.SparkSession

object adNestAnalyticsAgent extends LogMaster {

  def run(advertisementAnalyticsBuilder: AdvertisementAnalyticsBuilder)(implicit spark: SparkSession) = {

    val x = advertisementAnalyticsBuilder.buildOt._1
    StorageHandler.storeAsParquet(x)

    val y = advertisementAnalyticsBuilder.buildOt._1
    StorageHandler.storeAsParquet(y)

  }


  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = SparkContainer.initializeSparkSession("dev")

    val dataFrameLoader = DataFrameLoader()(spark)

    val advertisementAnalyticsBuilder = AdvertisementAnalyticsBuilder(dataFrameLoader)

    run(advertisementAnalyticsBuilder)(spark)

  }

}
