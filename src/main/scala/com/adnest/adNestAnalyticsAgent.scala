package com.adnest

import com.adnest.core.builder.AdvertisementAnalyticsBuilder
import com.adnest.core.storageHandler.StorageHandler
import com.adnest.core.dataLoader.DataFrameLoader
import com.adnest.utils.{LogMaster, SparkContainer}
import org.apache.spark.sql.SparkSession

object adNestAnalyticsAgent extends LogMaster {

  def run(advertisementAnalyticsBuilder: AdvertisementAnalyticsBuilder)(implicit spark: SparkSession): Unit = {

    advertisementAnalyticsBuilder.clicksProcessedCounts.show(100,false)
    advertisementAnalyticsBuilder.impressionsProcessedCounts.show(100,false)
    advertisementAnalyticsBuilder.avgTimeBetweenEventsForClicks.show(100, false)
    advertisementAnalyticsBuilder.avgTimeBetweenEventsForImpressions.show(100, false)

  }


  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = SparkContainer.initializeSparkSession("dev")

    val dataFrameLoader = DataFrameLoader()(spark)

    val advertisementAnalyticsBuilder = AdvertisementAnalyticsBuilder(dataFrameLoader)

    run(advertisementAnalyticsBuilder)(spark)

    spark.stop()

  }

}
