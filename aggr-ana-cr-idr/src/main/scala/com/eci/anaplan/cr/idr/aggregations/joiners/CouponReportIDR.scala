package com.eci.anaplan.cr.idr.aggregations.joiners

import com.eci.anaplan.cr.idr.aggregations.constructors.{CouponReportDf, ExchangeRateDf, CouponProductDf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import javax.inject.{Inject, Singleton}

@Singleton
class CouponReportIDR @Inject()(spark: SparkSession,
                                couponReportDf: CouponReportDf,
                                exchangeRateDf: ExchangeRateDf,
                                couponProductDf: CouponProductDf) {

  private def joinDataFrames: DataFrame = {

    import spark.implicits._

    couponReportDf.get.as("cr")
      .join(exchangeRateDf.get.as("coupon_rate"),
        $"cr.currency" === $"coupon_rate.from_currency"
          && $"cr.issued_date_formatted" === $"coupon_rate.conversion_date"
        , "left")
      .join(couponProductDf.get.as("cp"),
        $"cr.redeemed_booking_id" === $"cp.booking_id",
        "left"
      )
      .select(
        $"cr.coupon_id",
        $"cr.coupon_code",
        $"cr.coupon_issuer",
        $"cr.currency",
        $"cr.issued_date",
        $"cp.nonrefundable_date",
        $"cr.issued_date_formatted",
        coalesce(
          when($"cp.nonrefundable_date_formatted" === "" || $"cp.nonrefundable_date_formatted".isNull,
            $"cr.issued_date_formatted"
          ).otherwise(
            $"cp.nonrefundable_date_formatted"
          )
        ).as("nonrefundable_date_formatted"),
        $"cr.redeemed_booking_id",
        $"cr.redeemed_product_type",
        $"cr.coupon_full_amount",
        $"cr.coupon_allocated_amount",
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

  def joinWithColumn(): DataFrame =
    joinDataFrames
}