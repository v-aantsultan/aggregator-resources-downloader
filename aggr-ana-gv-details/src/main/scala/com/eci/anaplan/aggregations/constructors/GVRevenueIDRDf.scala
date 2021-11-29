package com.eci.anaplan.aggregations.constructors

import com.eci.anaplan.services.S3SourceService
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

// TODO: Update TestDataFrame1 and queries required
@Singleton
class GVRevenueIDRDf @Inject()(val sparkSession: SparkSession, s3SourceService: S3SourceService,
                               ExchangeRateDf: ExchangeRateDf) {

  import sparkSession.implicits._

  def get: DataFrame = {

    // TODO : Update this part of the code to get Domain data from S3
    s3SourceService.GVRevenueDf.as("gv_revenue")
      .join(ExchangeRateDf.get.as("exchange_rate_idr"),
        $"gv_revenue.revenue_currency" === $"exchange_rate_idr.from_currency"
          && to_date($"gv_revenue.revenue_date" + expr("INTERVAL 7 HOURS")) === to_date($"exchange_rate_idr.conversion_date")
        , "left")

      .select(
        to_date($"gv_revenue.revenue_date" + expr("INTERVAL 7 HOURS")).as("date"),
        when($"gv_revenue.status" === "REDEEMED","UNUSED VOUCHER")
          .otherwise($"gv_revenue.status").as("product"),
        lit("Traveloka").as("business_partner"),
        lit("None").as("voucher_redemption_product"),
        when($"gv_revenue.transaction_type" === "B2B","B2B")
          .otherwise(substring($"gv_revenue.revenue_currency",1,2)).as("customer"),
        lit("None").as("payment_channel_name"),
        when($"gv_revenue.revenue_currency" === "IDR",$"gv_revenue.gift_voucher_amount")
          .otherwise($"gv_revenue.gift_voucher_amount" * $"exchange_rate_idr.conversion_rate")
          .as("gift_voucher_amount"),
        $"gv_revenue.redeemed_booking_id".as("no_of_transactions"),
        $"gv_revenue.gift_voucher_id".as("no_gift_voucher"),
        when($"gv_revenue.revenue_currency" === "IDR",$"gv_revenue.revenue_amount")
          .otherwise($"gv_revenue.revenue_amount" * $"exchange_rate_idr.conversion_rate")
          .as("revenue_amount"),
        lit(0).as("unique_code"),
        lit(0).as("coupon_value"),
        lit(0).as("discount"),
        lit(0).as("premium")
      )
  }
}
