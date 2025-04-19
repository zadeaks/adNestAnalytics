package com.adnest.core.builder

import com.adnest.core.dataLoader.DataFrameLoader
import com.adnest.utils.LogMaster
import org.apache.spark.sql.functions.col

class AdvertisementAnalyticsBuilder(dataFrameLoader: DataFrameLoader) extends LogMaster {

  val clickProcessedDF = dataFrameLoader.clickProcessedDF
  val impressionsProcessedDF = dataFrameLoader.impressionsProcessedDF


  def buildOt = {
    val clickProcessedInputCols = clickProcessedDF.select(col("interaction_id"),
      col("transaction_header.creation_time").alias("creation_time"),
      col("device_settings.user_agent").alias("agent_id"))

    val impressionsProcessedInputCols = impressionsProcessedDF.select(
      col("interaction_id"),
      col("transaction_header.creation_time").alias("creation_time"),
      col("device_settings.user_agent").alias("agent_id"))
    (clickProcessedInputCols,impressionsProcessedInputCols)
  }
}

object AdvertisementAnalyticsBuilder {

  def apply(dataFrameLoader: DataFrameLoader) : AdvertisementAnalyticsBuilder = new AdvertisementAnalyticsBuilder(dataFrameLoader)

}
