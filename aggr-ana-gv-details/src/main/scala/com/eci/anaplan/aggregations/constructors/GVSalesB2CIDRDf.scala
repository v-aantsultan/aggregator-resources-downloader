package com.eci.anaplan.aggregations.constructors

import com.eci.anaplan.services.GVDetailsSource
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import javax.inject.{Inject, Singleton}

@Singleton
class GVSalesB2CIDRDf @Inject()(val sparkSession: SparkSession, s3SourceService: GVDetailsSource,
                                ExchangeRateDf: GVDetailsRateDf, GVIssuedlisteDf: GVIssuedIDRDf,
                                GVDetailsMDRDf: GVDetailsMDRDf) {

  import sparkSession.implicits._

  def get: DataFrame = {
    s3SourceService.GVSalesB2CDf.as("gv_sales_b2c")
      .join(ExchangeRateDf.get.as("exchange_rate_idr"),
        $"gv_sales_b2c.invoice_currency" === $"exchange_rate_idr.from_currency"
          && to_date($"gv_sales_b2c.issued_date" + expr("INTERVAL 7 HOURS")) === $"exchange_rate_idr.conversion_date"
        , "left")
      .join(GVIssuedlisteDf.get.as("gv_issuedlist"),
        $"gv_sales_b2c.booking_id" === $"gv_issuedlist.transaction_id"
        , "left")
      .join(GVDetailsMDRDf.get.as("mdr"),
        $"gv_sales_b2c.booking_id" === $"mdr.booking_id"
        , "left")

      .withColumn("count_bid",
        count($"gv_sales_b2c.booking_id").over(Window.partitionBy($"gv_sales_b2c.booking_id"))
      )
      .withColumn("mdr_charges_prorate",
        $"mdr.mdr_amount" / $"count_bid"
      )

      .select(
        to_date($"gv_sales_b2c.issued_date" + expr("INTERVAL 7 HOURS")).as("report_date"),
        lit("Purchase").as("product"),
        lit("Traveloka").as("business_partner"),
        lit("None").as("voucher_redemption_product"),
        substring($"gv_sales_b2c.invoice_currency",1,2).as("customer"),
        coalesce($"gv_sales_b2c.payment_channel_name",lit("None")).as("payment_channel_name"),
        when($"gv_sales_b2c.invoice_currency" === "IDR",$"gv_sales_b2c.gift_voucher_amount")
          .otherwise($"gv_sales_b2c.gift_voucher_amount" * $"exchange_rate_idr.conversion_rate")
          .as("gift_voucher_amount"),
        $"gv_sales_b2c.booking_id".as("no_of_transactions"),
        $"gv_issuedlist.gift_voucher_id".as("no_gift_voucher"),
        lit(0).as("revenue_amount"),
        when($"gv_sales_b2c.invoice_currency" === "IDR",$"gv_sales_b2c.unique_code")
          .otherwise($"gv_sales_b2c.unique_code" * $"exchange_rate_idr.conversion_rate")
          .as("unique_code"),
        when($"gv_sales_b2c.invoice_currency" === "IDR",$"gv_sales_b2c.coupon_value")
          .otherwise($"gv_sales_b2c.coupon_value" * $"exchange_rate_idr.conversion_rate")
          .as("coupon_value"),
        when($"gv_sales_b2c.invoice_currency" === "IDR",$"gv_sales_b2c.discount_or_premium")
          .otherwise($"gv_sales_b2c.discount_or_premium" * $"exchange_rate_idr.conversion_rate")
          .as("discount_or_premium_in_idr"),
        when($"gv_sales_b2c.invoice_currency" === "IDR",$"gv_sales_b2c.discount_wht")
          .otherwise($"gv_sales_b2c.discount_wht" * $"exchange_rate_idr.conversion_rate")
          .as("discount_wht_in_idr"),
        when($"gv_sales_b2c.invoice_currency" === "IDR",$"mdr_charges_prorate")
          .otherwise($"mdr_charges_prorate" * $"exchange_rate_idr.conversion_rate")
          .as("mdr_amount_idr")
      )
  }
}
