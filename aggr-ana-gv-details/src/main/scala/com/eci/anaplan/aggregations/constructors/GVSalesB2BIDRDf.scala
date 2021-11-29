package com.eci.anaplan.aggregations.constructors

import com.eci.anaplan.services.S3SourceService
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

// TODO: Update TestDataFrame1 and queries required
@Singleton
class GVSalesB2BIDRDf @Inject()(val sparkSession: SparkSession, s3SourceService: S3SourceService,
                                ExchangeRateDf: ExchangeRateDf) {

  import sparkSession.implicits._

  def get: DataFrame = {

    // TODO : Update this part of the code to get Domain data from S3
    s3SourceService.GVSalesB2BDf.as("gv_sales_b2b")
      .join(ExchangeRateDf.get.as("exchange_rate_idr"),
        $"gv_sales_b2b.gift_voucher_currency" === $"exchange_rate_idr.from_currency"
          && to_date($"gv_sales_b2b.report_date" + expr("INTERVAL 7 HOURS")) === to_date($"exchange_rate_idr.conversion_date")
        , "left")

      .select(
        to_date($"gv_sales_b2b.report_date" + expr("INTERVAL 7 HOURS")).as("date"),
        lit("Purchase").as("product"),
        lit("Traveloka").as("business_partner"),
        lit("None").as("voucher_redemption_product"),
        lit("B2B").as("customer"),
        lit("None").as("payment_channel_name"),
        when($"gv_sales_b2b.gift_voucher_currency" === "IDR",$"gv_sales_b2b.gift_voucher_nominal")
          .otherwise($"gv_sales_b2b.gift_voucher_nominal" * $"exchange_rate_idr.conversion_rate")
          .as("gift_voucher_amount"),
        $"gv_sales_b2b.movement_id".as("no_of_transactions"),
        $"gv_sales_b2b.gift_voucher_id".as("no_gift_voucher"),
        lit(0).as("revenue_amount"),
        lit(0).as("unique_code"),
        lit(0).as("coupon_value"),
        when($"gv_sales_b2b.gift_voucher_currency" === "IDR",$"gv_sales_b2b.discount_amount")
          .otherwise($"gv_sales_b2b.discount_amount" * $"exchange_rate_idr.conversion_rate")
          .as("discount_amount_in_idr"),
        when($"gv_sales_b2b.gift_voucher_currency" === "IDR",$"gv_sales_b2b.discount_wht")
          .otherwise($"gv_sales_b2b.discount_wht" * $"exchange_rate_idr.conversion_rate")
          .as("discount_wht_in_idr"),
        lit(0).as("premium")

      )
  }
}
