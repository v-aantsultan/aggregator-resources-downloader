package com.eci.anaplan.bp.details.aggregations.constructors

import com.eci.anaplan.bp.details.services.S3SourceService
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{coalesce, count, expr, lit, regexp_replace, size, split, substring, sum, to_date, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class BPDetailsDf @Inject()(val sparkSession: SparkSession, s3SourceService: S3SourceService,
                            ExchangeRateDf: ExchangeRateDf, MDRChargesDf: MDRChargesDf) {

  import sparkSession.implicits._

  def get: DataFrame = {
    s3SourceService.BPDetailsDf.as("bp")
      .join(ExchangeRateDf.get.as("invoice_rate"),
        $"bp.selling_currency" === $"invoice_rate.from_currency"
          && to_date($"bp.booking_issue_date" + expr("INTERVAL 7 HOURS")) === $"invoice_rate.conversion_date"
        ,"left")
      .join(MDRChargesDf.get.as("mdr"),
        $"bp.booking_id" === $"mdr.booking_id"
        ,"left")

      .withColumn("payment_scope_clear",
        regexp_replace($"bp.payment_scope","adjustment.*,","")
      )
      .withColumn("count_bid",
        count($"bp.booking_id").over(Window.partitionBy($"bp.booking_id"))
      )
      .withColumn("sum_customer_invoice_bid",
        sum($"bp.customer_invoice").over(Window.partitionBy($"bp.booking_id"))
      )
      .withColumn("mdr_amount_prorate",
        (($"sum_customer_invoice_bid" / $"mdr.expected_amount") * $"mdr.mdr_amount") / $"count_bid"
      )
      .withColumn("coupon_code_split",
        when(size(split($"bp.coupon_code",",")) === 0,1)
          .otherwise(size(split($"bp.coupon_code",",")))
      )

      .select(
        to_date($"bp.booking_issue_date" + expr("INTERVAL 7 HOURS")).as("report_date"),
        substring($"bp.selling_currency",1,2).as("customer"),
        $"bp.business_model".as("business_model"),
        $"bp.fulfillment_id".as("business_partner"),
        $"bp.product_category".as("product_category"),
        split($"payment_scope_clear",",")(0).as("payment_channel"),
        $"bp.booking_id".as("booking_id"),
        $"count_bid",
        coalesce(when($"bp.coupon_code" === "N/A" || $"bp.coupon_code".isNull
          || $"bp.coupon_code" === "",0)
            .otherwise($"coupon_code_split"))
          .as("coupon_code_result"),
        $"bp.published_rate_in_selling_currency".as("published_rate_in_selling_currency"),
        $"bp.commission_revenue".as("commission_revenue"),
        $"bp.transaction_fee".as("transaction_fee"),
        $"bp.discount_or_premium".as("discount_or_premium"),
        $"bp.discount_wht".as("discount_wht"),
        $"bp.unique_code".as("unique_code"),
        $"bp.total_coupon_value".as("total_coupon_value"),
        $"bp.point_redemption".as("point_redemption"),
        coalesce(when($"bp.selling_currency" === "IDR",$"mdr_amount_prorate")
          .otherwise($"mdr_amount_prorate" * $"invoice_rate.conversion_rate"),lit(0))
          .as("mdr_amount_prorate_idr")
      )
  }
}