package com.eci.anaplan.hs.details.aggregations.constructors

import com.eci.anaplan.hs.aggregations.joiners.HealthServiceIDR
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class HSDetailsDf @Inject()(val sparkSession: SparkSession,
                            healthServiceIDR: HealthServiceIDR) {

  import sparkSession.implicits._

  def get: DataFrame = {
    healthServiceIDR.joinWithColumn()

      .withColumn("payment_channel_clear",
        regexp_replace(
          regexp_replace($"payment_channel",
            "adjustment,",""),
          "adjustment_refund,","")
      )
      .withColumn("coupon_code_split",
        when(size(split($"coupon_code",",")) === 0,1)
          .otherwise(size(split($"coupon_code",",")))
      )

      .select(
        to_date($"booking_issue_date" + expr("INTERVAL 7 HOURS")).as("report_date"),
        substring($"locale",-2,2).as("customer"),
        $"business_model".as("business_model"),
        $"fulfillment_id".as("business_partner"),
        lit("HEALTHCARE").as("product"),
        upper($"category").as("product_category"),
        split($"payment_channel_clear",",")(0).as("payment_channel"),
        $"booking_id".as("booking_id"),
        coalesce(when($"coupon_code" === "N/A" || $"coupon_code".isNull
          || $"coupon_code" === "",0)
            .otherwise($"coupon_code_split"))
          .as("coupon_code_result"),
        $"number_of_tickets".as("number_of_tickets"),
        coalesce(
          when(upper($"business_model") === "CONSIGNMENT",$"recommended_price_contract_currency")
            .otherwise($"published_rate_contract_currency"),lit(0))
          .as("gmv"),
        coalesce(
          when(upper($"business_model") === "CONSIGNMENT",$"actual_gross_commission_contract_currency")
            .otherwise(0),lit(0))
          .as("actual_gross_commission_contract_currency"),
        coalesce(
          when($"discount_premium_incoming_fund_currency" <= 0,
            $"discount_premium_incoming_fund_currency" + $"discount_tax_expense")
            .otherwise(0),lit(0))
          .as("discount"),
        coalesce(
          when($"discount_premium_incoming_fund_currency" >= 0,
            $"discount_premium_incoming_fund_currency")
            .otherwise(0),lit(0))
          .as("premium"),
        coalesce($"unique_code",lit(0)).as("unique_code"),
        coalesce($"coupon_value",lit(0)).as("coupon_value"),
        coalesce(
          when(upper($"business_model") === "CONSIGNMENT",0)
            .otherwise($"published_rate_contract_currency"),lit(0))
          .as("gross_sales"),
        coalesce(
          when(upper($"business_model") === "CONSIGNMENT",0)
            .otherwise($"nta_contract_currency"),lit(0))
          .as("nta"),
        coalesce($"transaction_fee",lit(0)).as("transaction_fee"),
        coalesce($"point_redemption",lit(0)).as("point_redemption"),
        coalesce($"vat_out_contract_currency" * -1,lit(0)).as("vat_out"),
        coalesce($"mdr_charges" * -1,lit(0)).as("mdr_charges")
      )
  }
}