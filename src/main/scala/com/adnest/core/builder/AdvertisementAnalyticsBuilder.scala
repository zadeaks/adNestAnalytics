package com.adnest.core.builder

import com.adnest.core.dataLoader.DataFrameLoader
import com.adnest.utils.LogMaster
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

class AdvertisementAnalyticsBuilder(dataFrameLoader: DataFrameLoader, userAgent:String) extends LogMaster {

  val clickProcessedDF = dataFrameLoader.clickProcessedDF
  val impressionsProcessedDF = dataFrameLoader.impressionsProcessedDF

//  calculates the count of impressions and clicks by date and each hour of the day, for a specific user-agent
val clickProcessedInputCols = clickProcessedDF
  .select(
    col("transaction_header.creation_time").alias("creation_time"),
    col("device_settings.user_agent").alias("agent_id"))
  .na.fill(Map("creation_time" -> (current_timestamp() / 1000).toString(), "agent_id" -> "MyCustomAgent"))
  .withColumn("creation_time", from_unixtime(col("creation_time") / 1000))
  .withColumn("date", to_date(col("creation_time")))
  .withColumn("hour", hour(col("creation_time")))
  .filter(col("agent_id")===userAgent)


  val impressionsProcessedInputCols = impressionsProcessedDF
    .select(
    col("transaction_header.creation_time").alias("creation_time"),
    col("device_settings.user_agent").alias("agent_id"))
    .na.fill(Map("creation_time" -> (current_timestamp() / 1000).toString(), "agent_id" -> "MyCustomAgent"))
    .withColumn("creation_time", from_unixtime(col("creation_time") / 1000))
    .withColumn("date", to_date(col("creation_time")))
    .withColumn("hour", hour(col("creation_time")))
    .filter(col("agent_id")===userAgent)

  def clicksProcessedCounts: DataFrame = {
    clickProcessedInputCols
      .withColumn("clicks_count_by_hour",
        count("*")
          .over(Window
            .partitionBy("agent_id", "date", "hour")
            .orderBy("agent_id", "date", "hour")))
      .withColumn("clicks_count_by_date",
        count("*")
          .over(Window
            .partitionBy("agent_id", "date")
            .orderBy("agent_id", "date")))
      .withColumn("clicks_count_by_user_agent",
        count("*")
          .over(Window
            .partitionBy("agent_id")
            .orderBy("agent_id")))
      .distinct()
  }


  def impressionsProcessedCounts: DataFrame = {
    impressionsProcessedInputCols
      .withColumn("impressions_count_by_hour",
        count("*")
          .over(Window
            .partitionBy("agent_id", "date", "hour")
            .orderBy("agent_id", "date", "hour")))
      .withColumn("impressions_count_by_date",
        count("*")
          .over(Window
            .partitionBy("agent_id", "date")
            .orderBy("agent_id", "date")))
      .withColumn("impressions_count_by_user_agent",
        count("*")
          .over(Window
            .partitionBy("agent_id")
            .orderBy("agent_id")))
      .distinct()
  }

  def avgTimeBetweenEvents(df:DataFrame):DataFrame = {
    val prevTimeAdd = df
      .withColumn("prev_creation_time",
      lag(col("creation_time"),1)
        .over(Window.orderBy("agent_id", "date", "hour")))
      .distinct()
      .filter(col("prev_creation_time").isNotNull)

    val res = prevTimeAdd
      .groupBy("agent_id","creation_time")
      .agg(avg(unix_timestamp(col("creation_time"))-unix_timestamp(col("prev_creation_time")))
        .alias("avg_time_between_events_in_miliseconds"))
    res
  }

  val avgTimeBetweenEventsForClicks = avgTimeBetweenEvents(clickProcessedInputCols)
  val avgTimeBetweenEventsForImpressions = avgTimeBetweenEvents(impressionsProcessedInputCols)

}

object AdvertisementAnalyticsBuilder {

  def apply(dataFrameLoader: DataFrameLoader, userAgent:String) : AdvertisementAnalyticsBuilder = new AdvertisementAnalyticsBuilder(dataFrameLoader,userAgent)

}
