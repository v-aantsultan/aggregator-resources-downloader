package com.eci.anaplan.aggregations.constructors

import com.eci.anaplan.services.GVDetailsSource
import org.apache.spark.sql.functions.{expr, lit, substring, to_date, when}
import javax.inject.{Inject, Singleton}
import org.apache.spark.sql.{DataFrame, SparkSession}

// TODO: Update TestDataFrame1 and queries required
@Singleton
class GVRedeemIDRDf @Inject()(val sparkSession: SparkSession, s3SourceService: GVDetailsSource,
                              UnderlyingProductDf: GVDetailsUnderlyingProductDf,
                              ExchangeRateDf: GVDetailsRateDf) {

  import sparkSession.implicits._

  def get: DataFrame = {
    // TODO : Update this part of the code to get Domain data from S3
    s3SourceService.GVRedeemDf.as("gv_redeem")
      .join(ExchangeRateDf.get.as("exchange_rate_idr"),
          $"gv_redeem.gift_voucher_currency" === $"exchange_rate_idr.from_currency"
            && to_date($"gv_redeem.redemption_date" + expr("INTERVAL 7 HOURS")) === to_date($"exchange_rate_idr.conversion_date")
          , "left")
      .join(UnderlyingProductDf.get.as("underlying_product"),
          $"gv_redeem.redeemed_product_type" === $"underlying_product.fs_product_type"
          , "left")

      .select(
        to_date($"gv_redeem.redemption_date" + expr("INTERVAL 7 HOURS")).as("date"),
        lit("Redeemed").as("product"),
        lit("Traveloka").as("business_partner"),
        $"underlying_product.underlying_product".as("voucher_redemption_product"),
        when($"gv_redeem.transaction_type" === "B2B","B2B")
          .otherwise(substring($"gv_redeem.gift_voucher_currency",1,2)).as("customer"),
        lit("None").as("payment_channel_name"),
        when($"gv_redeem.gift_voucher_currency" === "IDR",$"gv_redeem.gift_voucher_redeemed_amount")
          .otherwise($"gv_redeem.gift_voucher_redeemed_amount" * $"exchange_rate_idr.conversion_rate")
          .as("gift_voucher_amount"),
        $"gv_redeem.redeemed_booking_id".as("no_of_transactions"),
        $"gv_redeem.gift_voucher_id".as("no_gift_voucher"),
        lit(0).as("revenue_amount"),
        lit(0).as("unique_code"),
        lit(0).as("coupon_value"),
        lit(0).as("discount"),
        lit(0).as("premium")
      )
  }
}
