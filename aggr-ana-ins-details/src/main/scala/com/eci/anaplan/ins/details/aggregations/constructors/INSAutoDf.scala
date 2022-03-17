package com.eci.anaplan.ins.details.aggregations.constructors

import com.eci.anaplan.ins.details.services.S3SourceService
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{coalesce, count, expr, lit, regexp_replace, split, substring, sum, to_date, when}
import org.apache.spark.sql.{DataFrame, SparkSession}
import javax.inject.{Inject, Singleton}

@Singleton
class INSAutoDf @Inject()(val sparkSession: SparkSession, s3SourceService: S3SourceService,
                          ExchangeRateDf: ExchangeRateDf,PurchaseDeliveryDf: PurchaseDeliveryDf,
                          MDRChargesDf: MDRChargesDf, MappingProductNameDf: MappingProductNameDf) {

  import sparkSession.implicits._

  def get: DataFrame = {
    s3SourceService.INSAutoDf.as("ins_auto")
      .join(ExchangeRateDf.get.as("invoice_rate"),
        $"ins_auto.invoice_currency" === $"invoice_rate.from_currency"
          && to_date($"ins_auto.recognition_date" + expr("INTERVAL 7 HOURS")) === $"invoice_rate.conversion_date"
        ,"left")
      .join(ExchangeRateDf.get.as("provider_rate"),
        $"ins_auto.provider_currency" === $"provider_rate.from_currency"
          && to_date($"ins_auto.recognition_date" + expr("INTERVAL 7 HOURS")) === $"provider_rate.conversion_date"
        ,"left")
      .join(PurchaseDeliveryDf.get.as("pd"),
        $"ins_auto.policy_id" === $"pd.policy_id"
        ,"left")
      .join(MDRChargesDf.get.as("mdr"),
        $"ins_auto.booking_id" === $"mdr.booking_id"
        ,"left")
      .join(MappingProductNameDf.get.as("prd_nm"),
        $"ins_auto.product_name" === $"prd_nm.product_name"
        ,"left")

      .withColumn("payment_scope_clear",
          regexp_replace($"ins_auto.payment_scope","adjustment.*,","")
      )
      .withColumn("count_bid",
        count($"ins_auto.booking_id").over(Window.partitionBy($"ins_auto.booking_id"))
      )
      .withColumn("sum_actual_fare_bid",
        sum($"ins_auto.total_actual_fare_paid_by_customer").over(Window.partitionBy($"ins_auto.booking_id"))
      )
      .withColumn("mdr_amount_prorate",
        (($"sum_actual_fare_bid" / $"mdr.expected_amount") * $"mdr.mdr_amount") / $"count_bid"
      )

      .select(
        to_date($"ins_auto.recognition_date" + expr("INTERVAL 7 HOURS")).as("report_date"),
        substring($"ins_auto.locale",-2,2).as("customer"),
        $"ins_auto.fulfillment_id".as("business_partner"),
        $"ins_auto.product_type".as("product"),
        when($"prd_nm.product_name_mapping".isNull,$"ins_auto.product_name")
          .otherwise($"prd_nm.product_name_mapping")
          .as("product_category"),
        split($"payment_scope_clear",",")(0).as("payment_channel"),
        $"ins_auto.booking_id".as("booking_id"),
        $"count_bid",
        $"ins_auto.policy_id".as("policy_id"),
        $"pd.num_of_coverage".as("num_of_coverage"),
        coalesce(when($"ins_auto.provider_currency" === "IDR",$"ins_auto.total_fare_from_provider")
          .otherwise($"ins_auto.total_fare_from_provider" * $"provider_rate.conversion_rate"),lit(0))
          .as("total_fare_from_provider_idr"),
        coalesce(when($"ins_auto.provider_currency" === "IDR",$"ins_auto.insurance_commission")
          .otherwise($"ins_auto.insurance_commission" * $"provider_rate.conversion_rate"),lit(0))
          .as("insurance_commission_idr"),
        coalesce(when($"ins_auto.provider_currency" === "IDR",$"ins_auto.total_other_income")
          .otherwise($"ins_auto.total_other_income" * $"provider_rate.conversion_rate"),lit(0))
          .as("total_other_income_idr"),
        coalesce(when($"ins_auto.invoice_currency" === "IDR",$"ins_auto.discount_or_premium")
          .otherwise($"ins_auto.discount_or_premium" * $"invoice_rate.conversion_rate"),lit(0))
          .as("discount_or_premium_idr"),
        coalesce(when($"ins_auto.invoice_currency" === "IDR",$"ins_auto.discount_wht_expense")
          .otherwise($"ins_auto.discount_wht_expense" * $"invoice_rate.conversion_rate"),lit(0))
          .as("discount_wht_expense_idr"),
        coalesce(when($"ins_auto.invoice_currency" === "IDR",$"ins_auto.unique_code")
          .otherwise($"ins_auto.unique_code" * $"invoice_rate.conversion_rate"),lit(0))
          .as("unique_code_idr"),
        coalesce(when($"ins_auto.invoice_currency" === "IDR",$"ins_auto.coupon_value")
          .otherwise($"ins_auto.coupon_value" * $"invoice_rate.conversion_rate"),lit(0))
          .as("coupon_value_idr"),
        coalesce(when($"ins_auto.invoice_currency" === "IDR",$"mdr_amount_prorate")
          .otherwise($"mdr_amount_prorate" * $"invoice_rate.conversion_rate"),lit(0))
          .as("mdr_amount_idr")

      )
  }
}