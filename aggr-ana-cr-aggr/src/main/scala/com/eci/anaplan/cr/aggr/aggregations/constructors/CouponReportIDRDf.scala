package com.eci.anaplan.cr.aggr.aggregations.constructors

import com.eci.anaplan.cr.aggr.aggregations.constructors.{CouponProductDf, CouponReportDf, ExchangeRateDf}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class CouponReportIDRDf @Inject()(val sparkSession: SparkSession,
                                  couponReportDf: CouponReportDf,
                                  exchangeRateDf: ExchangeRateDf,
                                  couponProductDf: CouponProductDf) {

  import sparkSession.implicits._

  def get: DataFrame = {

    couponReportDf.get.as("cr")
      .join(exchangeRateDf.get.as("coupon_rate"),
        $"cr.currency" === $"coupon_rate.from_currency"
          && $"cr.issued_date_formatted" === $"coupon_rate.conversion_date"
        , "left")
      .join(couponProductDf.get.as("cp"),
        $"cr.redeemed_booking_id" === $"cp.booking_id",
        "left"
      )
      .withColumn("coupon_booking_concate",
        concat($"cr.redeemed_booking_id", lit("&"), $"cr.coupon_code")
      )
      .withColumn("count_coupon_booking_concate",
        count($"coupon_booking_concate").over(Window.partitionBy($"coupon_booking_concate"))
      )
      .select(
        $"cr.coupon_id",
        $"cr.coupon_code".as("coupon_code"),
        $"cr.coupon_issuer",
        substring($"cr.currency",0,2).as("customer"),
        $"cr.issued_date",
        coalesce(
          when($"cp.nonrefundable_date_formatted".isNull || $"cp.nonrefundable_date_formatted" === "", $"cr.issued_date_formatted")
            .otherwise($"cp.nonrefundable_date_formatted")
        ).as("nonrefundable_date"),
        $"cr.issued_date_formatted",
        coalesce(
          when($"cp.nonrefundable_date_formatted" === "" || $"cp.nonrefundable_date_formatted".isNull,
            $"cr.issued_date_formatted"
          ).otherwise(
            $"cp.nonrefundable_date_formatted"
          )
        ).as("report_date"),
        $"cr.redeemed_booking_id".as("redeemed_booking_id"),
        $"cr.redeemed_product_type".as("product"),
        $"cr.coupon_full_amount",
        $"cr.coupon_allocated_amount",
        $"coupon_booking_concate".as("coupon_booking_concate"),
        $"count_coupon_booking_concate".as("count_coupon_booking_concate"),
        coalesce(
          when($"cr.currency" === "IDR", $"cr.coupon_full_amount")
            .otherwise($"cr.coupon_full_amount" * $"coupon_rate.conversion_rate"), lit(0)
        ).as("coupon_full_amount_idr"),
        coalesce(
          when($"cr.currency" === "IDR", $"cr.coupon_allocated_amount")
            .otherwise($"cr.coupon_allocated_amount" * $"coupon_rate.conversion_rate"),lit(0)
        ).as("coupon_allocated_amount_idr")
      )
  }
}