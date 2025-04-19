package com.adnest.core.storageHandler

import com.adnest.utils.LogMaster
import org.apache.spark.sql.DataFrame

object StorageHandler extends LogMaster {

  def storeAsParquet(df: DataFrame, numPartFiles:Int = 1) = {
    df.coalesce(numPartFiles)
      .write.format("parquet")
      .mode("append")
      .option("header", "true")
      .save("/output/parquet")
  }



}
