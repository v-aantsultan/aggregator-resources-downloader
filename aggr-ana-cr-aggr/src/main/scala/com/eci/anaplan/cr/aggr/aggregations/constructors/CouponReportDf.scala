package com.eci.anaplan.cr.aggr.aggregations.constructors

import com.eci.anaplan.cr.aggr.services.S3SourceService
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class CouponReportDf @Inject()(val sparkSession: SparkSession,
                               s3SourceService: S3SourceService
                              ) {

  import sparkSession.implicits._

  def get: DataFrame = {
    s3SourceService.CouponRedeemDf
      .select(
        $"coupon_id".as("coupon_id"),
        $"coupon_code".as("coupon_code"),
        $"coupon_issuer".as("coupon_issuer"),
        $"currency".as("currency"),
        $"issued_date".as("issued_date"),
        to_date($"issued_date" + expr("INTERVAL 7 HOURS")).as("issued_date_formatted"),
        $"redeemed_booking_id".as("redeemed_booking_id"),
        $"redeemed_product_type".as("redeemed_product_type"),
        $"coupon_full_amount".as("coupon_full_amount"),
        $"coupon_allocated_amount".as("coupon_allocated_amount")
      )
  }
}