package com.eci.anaplan.wm.details.aggregations.constructors

import com.eci.anaplan.wm.details.services.S3SourceService
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{coalesce, count, expr, lit, regexp_replace, size, split, substring, sum, to_date, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class WMDetailsDf @Inject()(val sparkSession: SparkSession, s3SourceService: S3SourceService,
                            ExchangeRateDf: ExchangeRateDf, MDRChargesDf: MDRChargesDf) {

  import sparkSession.implicits._

  def get: DataFrame = {
    s3SourceService.WMDetailsDf.as("wm")
      .join(ExchangeRateDf.get.as("invoice_rate"),
        $"wm.invoice_currency" === $"invoice_rate.from_currency"
          && to_date($"wm.booking_issued_date" + expr("INTERVAL 7 HOURS")) === $"invoice_rate.conversion_date"
        ,"left")
      .join(MDRChargesDf.get.as("mdr"),
        $"wm.booking_id" === $"mdr.booking_id"
        ,"left")

      .withColumn("payment_scope_clear",
        regexp_replace($"wm.payment_scope","adjustment.*,","")
      )
      .withColumn("count_bid",
        count($"wm.booking_id").over(Window.partitionBy($"wm.booking_id"))
      )
      .withColumn("sum_total_actual_fare_bid",
        sum($"wm.total_actual_fare_paid_by_customer").over(Window.partitionBy($"wm.booking_id"))
      )
      .withColumn("mdr_amount_prorate",
        (($"sum_total_actual_fare_bid" / $"mdr.expected_amount") * $"mdr.mdr_amount") / $"count_bid"
      )

      .select(
        to_date($"wm.booking_issued_date" + expr("INTERVAL 7 HOURS")).as("report_date"),
        substring($"wm.invoice_currency",1,2).as("customer"),
        $"wm.business_model".as("business_model"),
        $"wm.fulfillment_id".as("business_partner"),
        $"wm.product_type".as("product"),
        $"wm.product_name".as("product_category"),
        split($"payment_scope_clear",",")(0).as("payment_channel"),
        $"wm.booking_id".as("booking_id"),
        $"count_bid",
        coalesce(when($"wm.coupon_value" =!= 0, "wm.coupon_value")
          .otherwise(0),lit(0))
          .as("coupon_value_result"),
        $"wm.topup_amount".as("topup_amount"),
        $"wm.commission_revenue".as("commission_revenue"),
        $"wm.unique_code".as("unique_code"),
        $"wm.coupon_value".as("coupon_value"),
        $"wm.point_redemption".as("point_redemption"),
        coalesce(when($"wm.invoice_currency" === "IDR",$"mdr_amount_prorate")
          .otherwise($"mdr_amount_prorate" * $"invoice_rate.conversion_rate"),lit(0))
          .as("mdr_amount_prorate_idr")
      )
  }
}