package com.adnest.core.dataLoader

import com.adnest.utils.LogMaster
import org.apache.spark.sql.{DataFrame, SparkSession}

class DataFrameLoader(implicit spark: SparkSession) extends LogMaster {


  def parquetLoader(filePath: String):DataFrame = {
    logInfo(s"Loading parquet file at location : $filePath")
    val fileDF = spark.read.format("parquet").load(filePath)
    fileDF
  }

  val clickProcessedDF: DataFrame = parquetLoader("E:\\GitRepos\\spark-poc\\advertisement-clicks-and-revenue-analysis-using-deeply-nested-parquet-data\\src\\resources\\data\\clicks_processed_*.parquet")

  val impressionsProcessedDF: DataFrame = parquetLoader("E:\\GitRepos\\spark-poc\\advertisement-clicks-and-revenue-analysis-using-deeply-nested-parquet-data\\src\\resources\\data\\impressions_processed_dk_*.parquet")


}

object DataFrameLoader {

  def apply()(implicit spark: SparkSession) = new DataFrameLoader()(spark)

}
