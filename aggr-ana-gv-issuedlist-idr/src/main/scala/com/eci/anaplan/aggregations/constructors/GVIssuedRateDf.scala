package com.eci.anaplan.aggregations.constructors

import com.eci.anaplan.services.GVIssuedSource
import org.apache.spark.sql.functions.to_date
import org.apache.spark.sql.{DataFrame, SparkSession}
import javax.inject.{Inject, Singleton}

@Singleton
class GVIssuedRateDf @Inject()(val sparkSession: SparkSession, s3SourceService: GVIssuedSource) {

  import sparkSession.implicits._

  def get: DataFrame = {
    s3SourceService.ExchangeRateDf
      .filter($"`to_currency`" === "IDR")
      .select(
        $"`from_currency`".as("from_currency"),
        $"`to_currency`".as("to_currency"),
        to_date($"`conversion_date`").as("conversion_date"),
        $"`conversion_type`".as("conversion_type"),
        $"`conversion_rate`".as("conversion_rate")
      )
  }
}
