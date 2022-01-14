package com.eci.anaplan.aggregations.constructors

import com.eci.anaplan.services.GVDetailsSource
import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.{DataFrame, SparkSession}
import javax.inject.{Inject, Singleton}

@Singleton
class GVIssuedIDRDf @Inject()(val sparkSession: SparkSession, s3SourceService: GVDetailsSource) {

  import sparkSession.implicits._

  def get: DataFrame = {
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
