package com.eci.anaplan.cd.detail.aggregations.constructors

import com.eci.anaplan.cd.detail.services.S3SourceService
import com.eci.anaplan.cd.detail.aggregations.constructors.{ConnectivityDomesticDf, ExchangeRateDf, MDRChargesDf}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{substring, _}
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

//noinspection ScalaStyle
@Singleton
class ConnectivityDomesticDf @Inject()(val sparkSession: SparkSession,
                                       ExchangeRateDf: ExchangeRateDf,
                                       MDRChargesDf: MDRChargesDf,
                                       s3SourceService: S3SourceService) {

  import sparkSession.implicits._

  def get: DataFrame = {
    s3SourceService.ConnectivityDomesticDf.as("cd")
      .join(ExchangeRateDf.get.as("invoice_rate"),
        $"cd.selling_currency" === $"invoice_rate.from_currency"
          && to_date($"cd.booking_issue_date" + expr("INTERVAL 7 HOURS")) === $"invoice_rate.conversion_date"
        , "left")
      .join(MDRChargesDf.get.as("mdr"),
        $"cd.booking_id" === $"mdr.booking_id"
        , "left")
      .withColumn("count_bid",
        count($"cd.booking_id").over(Window.partitionBy($"cd.booking_id"))
      )
      .withColumn("sum_customer_invoice_bid",
        sum($"`customer_invoice`").over(Window.partitionBy($"cd.booking_id"))
      )
      .withColumn("mdr_amount_prorate",
        (($"sum_customer_invoice_bid" / $"mdr.expected_amount") * $"mdr.mdr_amount") / $"count_bid"
      )
      .withColumn("coupon_code_split",
        when(size(split($"cd.coupon_code", ",")) === 0, 1)
          .otherwise(size(split($"cd.coupon_code", ",")))
      )
      .select(
        to_date($"cd.booking_issue_date" + expr("INTERVAL 7 HOURS")).as("report_date"),
        substring($"cd.selling_currency", 0, 2).as("customer"),
        coalesce(
          when($"cd.fulfillment_id" === "" || $"cd.fulfillment_id".isNull, "3rd Party")
            .otherwise($"cd.fulfillment_id")
        ).as("business_partner"),
        $"cd.business_model".as("business_model"),
        $"cd.product_category".as("product_category"),
        coalesce(
          when($"cd.payment_scope".isNull, "None")
            .otherwise(split($"cd.payment_scope", "\\,").getItem(0))
        ).as("payment_channel"),
        coalesce(
          when($"cd.coupon_code" === "N/A" || $"cd.coupon_code".isNull || $"cd.coupon_code" === "", 0)
            .otherwise($"coupon_code_split")
        ).as("coupon_code_result"),
        $"cd.discount_or_premium".as("discount_or_premium"),
        $"cd.discount_wht".as("discount_wht"),
        $"cd.premium".as("premium"),
        $"cd.net_to_agent".as("net_to_agent"),
        $"cd.booking_id".as("booking_id"),
        $"cd.commission_revenue".as("commission_revenue"),
        $"cd.transaction_fee".as("transaction_fee"),
        $"cd.unique_code".as("unique_code"),
        $"cd.point_redemption".as("point_redemption"),
        $"cd.total_coupon_value".as("total_coupon_value"),
        $"cd.published_rate_in_selling_currency".as("published_rate_in_selling_currency"),
        coalesce($"mdr_amount_prorate", lit(0)).as("mdr_amount_prorate"),
        coalesce(when($"cd.selling_currency" === "IDR", $"mdr_amount_prorate")
          .otherwise($"mdr_amount_prorate" * $"invoice_rate.conversion_rate"), lit(0))
          .as("mdr_amount_prorate_idr")
      )
  }
}