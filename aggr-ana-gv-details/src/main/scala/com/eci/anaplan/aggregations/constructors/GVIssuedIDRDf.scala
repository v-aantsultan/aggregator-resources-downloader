package com.eci.anaplan.aggregations.constructors

import com.eci.anaplan.services.GVDetailsSource
import org.apache.spark.sql.functions.{countDistinct, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

// TODO: Update TestDataFrame1 and queries required
@Singleton
class GVIssuedIDRDf @Inject()(val sparkSession: SparkSession, s3SourceService: GVDetailsSource) {

  import sparkSession.implicits._

  def get: DataFrame = {
    // TODO : Update this part of the code to get Domain data from S3
    s3SourceService.GVIssuedlistDf
      .groupBy($"transaction_id")
      .agg(
        countDistinct($"gift_voucher_id").as("gift_voucher_id")
      )
      .select(
        $"*"
      )
  }
}
