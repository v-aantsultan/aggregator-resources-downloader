package com.eci.anaplan.ins.details.aggregations.constructors

import com.eci.anaplan.ins.details.services.S3SourceService
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import javax.inject.{Inject, Singleton}

@Singleton
class INSNonAutoDf @Inject()(val sparkSession: SparkSession, s3SourceService: S3SourceService,
                             ExchangeRateDf: ExchangeRateDf, PurchaseDeliveryItemDf: PurchaseDeliveryItemDf,
                             MDRChargesDf: MDRChargesDf) {

  import sparkSession.implicits._

  def get: DataFrame = {
    s3SourceService.INSNonAutoDf.as("ins_nonauto")
      .join(ExchangeRateDf.get.as("invoice_rate"),
        $"ins_nonauto.invoice_currency" === $"invoice_rate.from_currency"
          && to_date($"ins_nonauto.booking_issued_date" + expr("INTERVAL 7 HOURS")) === $"invoice_rate.conversion_date"
        ,"left")
      .join(ExchangeRateDf.get.as("provider_rate"),
        $"ins_nonauto.provider_currency" === $"provider_rate.from_currency"
          && to_date($"ins_nonauto.booking_issued_date" + expr("INTERVAL 7 HOURS")) === $"provider_rate.conversion_date"
        ,"left")
      .join(PurchaseDeliveryItemDf.get.as("pdi"),
        $"ins_nonauto.policy_id" === $"pdi.policy_id"
        ,"left")
      .join(MDRChargesDf.get.as("mdr"),
        $"ins_nonauto.booking_id" === $"mdr.booking_id"
        ,"left")

      .withColumn("payment_scope_clear",
          regexp_replace($"ins_nonauto.payment_scope","adjustment.*,","")
      )
      .withColumn("count_bid",
        count($"ins_nonauto.booking_id").over(Window.partitionBy($"ins_nonauto.booking_id"))
      )
      .withColumn("mdr_amount_prorate",
        $"mdr.mdr_amount" / $"count_bid"
      )

      .select(
        to_date($"ins_nonauto.booking_issued_date" + expr("INTERVAL 7 HOURS")).as("report_date"),
        substring($"ins_nonauto.locale",-2,2).as("customer"),
        $"ins_nonauto.fulfillment_id".as("business_partner"),
        when($"ins_nonauto.booking_type".isin("CROSSSELL_ADDONS","ADDONS","CROSSSELL_BUNDLE"),lit("IA"))
          .otherwise(lit("IS")).as("product"),
        $"ins_nonauto.product_name".as("product_category"),
        split($"payment_scope_clear",",")(0).as("payment_channel"),
        $"ins_nonauto.booking_id".as("booking_id"),
        $"count_bid",
        $"ins_nonauto.policy_id".as("policy_id"),
        $"pdi.num_of_coverage".as("num_of_coverage"),
        coalesce(when($"ins_nonauto.provider_currency" === "IDR",$"ins_nonauto.total_fare_from_provider")
          .otherwise($"ins_nonauto.total_fare_from_provider" * $"provider_rate.conversion_rate"),lit(0))
          .as("total_fare_from_provider_idr"),
        coalesce(when($"ins_nonauto.provider_currency" === "IDR",$"ins_nonauto.insurance_commission")
          .otherwise($"ins_nonauto.insurance_commission" * $"provider_rate.conversion_rate"),lit(0))
          .as("insurance_commission_idr"),
        coalesce(when($"ins_nonauto.provider_currency" === "IDR",$"ins_nonauto.total_other_income")
          .otherwise($"ins_nonauto.total_other_income" * $"provider_rate.conversion_rate"),lit(0))
          .as("total_other_income_idr"),
        coalesce(when($"ins_nonauto.invoice_currency" === "IDR",$"ins_nonauto.discount_or_premium")
          .otherwise($"ins_nonauto.discount_or_premium" * $"invoice_rate.conversion_rate"),lit(0))
          .as("discount_or_premium_idr"),
        coalesce(when($"ins_nonauto.invoice_currency" === "IDR",$"ins_nonauto.discount_wht_expense")
          .otherwise($"ins_nonauto.discount_wht_expense" * $"invoice_rate.conversion_rate"),lit(0))
          .as("discount_wht_expense_idr"),
        coalesce(when($"ins_nonauto.invoice_currency" === "IDR",$"ins_nonauto.unique_code")
          .otherwise($"ins_nonauto.unique_code" * $"invoice_rate.conversion_rate"),lit(0))
          .as("unique_code_idr"),
        coalesce(when($"ins_nonauto.invoice_currency" === "IDR",$"ins_nonauto.coupon_value")
          .otherwise($"ins_nonauto.coupon_value" * $"invoice_rate.conversion_rate"),lit(0))
          .as("coupon_value_idr"),
        coalesce(when($"ins_nonauto.invoice_currency" === "IDR",$"mdr_amount_prorate")
          .otherwise($"mdr_amount_prorate" * $"invoice_rate.conversion_rate"),lit(0))
          .as("mdr_amount_idr")
      )
  }
}