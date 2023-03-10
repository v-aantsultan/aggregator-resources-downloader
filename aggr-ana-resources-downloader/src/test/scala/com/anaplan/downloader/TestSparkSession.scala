package com.anaplan.downloader

import org.apache.spark.sql.SparkSession

trait TestSparkSession {
  // Refer to holden library the config used by him
  lazy val testSparkSession: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("spark test")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.host", "localhost")
      .getOrCreate()
  }
}
