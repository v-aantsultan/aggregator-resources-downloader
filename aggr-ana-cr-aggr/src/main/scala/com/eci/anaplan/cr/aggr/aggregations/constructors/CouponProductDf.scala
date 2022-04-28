package com.eci.anaplan.cr.aggr.aggregations.constructors

import com.eci.anaplan.cr.aggr.services.S3SourceService
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class CouponProductDf @Inject()(val sparkSession: SparkSession,
                                s3SourceService: S3SourceService
                              ) {

  import sparkSession.implicits._

  def get: DataFrame = {
    s3SourceService.CouponProductDf
      .select(
        $"booking_id".as("booking_id"),
        $"product_type".as("product_type"),
        $"non_refundable_date".as("nonrefundable_date"),
        to_date($"non_refundable_date" + expr("INTERVAL 7 HOURS")).as("nonrefundable_date_formatted"),
        $"issued_time".as("issued_time"),
        $"invoice_due_date".as("invoice_due_date")
      )
  }
}