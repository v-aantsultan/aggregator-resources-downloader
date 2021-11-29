package com.eci.anaplan.aggregations.constructors

import com.eci.anaplan.services.S3SourceService
import org.apache.spark.sql.{DataFrame, SparkSession}
import javax.inject.{Inject, Singleton}

// TODO: Update TestDataFrame1 and queries required
@Singleton
class GVIssuedlisteDf @Inject()(val sparkSession: SparkSession, s3SourceService: S3SourceService) {

  import sparkSession.implicits._

  def get: DataFrame = {
    // TODO : Update this part of the code to get Domain data from S3
    s3SourceService.GVIssuedlistDf
      .select(
        $"`transaction_id`".as("transaction_id"),
        $"`gift_voucher_id`".as("gift_voucher_id")
      )
  }
}
