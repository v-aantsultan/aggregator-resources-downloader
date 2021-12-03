package com.eci.anaplan.aggregations.constructors

import com.eci.anaplan.services.GVRedeemSource
import org.apache.spark.sql.{DataFrame, SparkSession}
import javax.inject.{Inject, Singleton}

// TODO: Update TestDataFrame1 and queries required
@Singleton
class GVRedeemRateDf @Inject()(val sparkSession: SparkSession,
                               s3SourceService: GVRedeemSource) {

  import sparkSession.implicits._

  def get: DataFrame = {
    // TODO : Update this part of the code to get Domain data from S3
    s3SourceService.ExchangeRateDf
      .filter($"`to_currency`" === "IDR")
      .select(
        $"`from_currency`".as("from_currency"),
        $"`to_currency`".as("to_currency"),
        $"`conversion_date`".as("conversion_date"),
        $"`conversion_type`".as("conversion_type"),
        $"`conversion_rate`".as("conversion_rate")
      )
  }
}
