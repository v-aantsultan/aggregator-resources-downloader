package com.eci.anaplan

import org.apache.spark.sql.DataFrame

trait CsvLoader extends TestSparkSession {
  def getCsv(path: String, alias: String): DataFrame = {
    testSparkSession.read
      .option("header", "true")
      .csv(path)
      .as(alias)
  }
}
