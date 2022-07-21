package com.eci.anaplan.fa.details.aggregations.constructors

import com.eci.anaplan.fa.details.services.S3SourceService
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class FlightAncillariesDetailsDf @Inject()(val sparkSession: SparkSession,
                                           s3SourceService: S3SourceService,
                                           MDRChargesDf: MDRChargesDf,
                                           exchangeRateDf: ExchangeRateDf,
                                           mappingFulfillmentIDDf: MappingFulfillmentIDDf) {

  import sparkSession.implicits._

  def get: DataFrame = {
    s3SourceService.FADf.as("fa")
      .join(mappingFulfillmentIDDf.get.as("ful_id"),
        $"fa.fulfillment_id" === $"ful_id.fulfillment_id"
        ,"left")
      .join(exchangeRateDf.get.as("collecting_rate"),
        $"fa.collecting_currency" === $"collecting_rate.from_currency"
          && to_date($"fa.booking_issue_date","dd/MM/yyyy") === $"collecting_rate.conversion_date"
        ,"left")
      .join(exchangeRateDf.get.as("contract_rate"),
        $"fa.contract_currency" === $"contract_rate.from_currency"
          && to_date($"fa.booking_issue_date","dd/MM/yyyy") === $"contract_rate.conversion_date"
        ,"left")
      .join(MDRChargesDf.get.as("mdr"),
        $"fa.booking_id" === $"mdr.booking_id"
        ,"left")

      .withColumn("count_bid",
        count($"fa.booking_id").over(Window.partitionBy($"fa.booking_id"))
      )
      .withColumn("sum_customer_invoice_bid",
        sum($"fa.customer_invoice").over(Window.partitionBy($"fa.booking_id"))
      )
      .withColumn("mdr_charges",
        (($"sum_customer_invoice_bid" / $"mdr.expected_amount") * $"mdr.mdr_amount") / $"count_bid"
      )
      .withColumn("supplier_name_concat",
        concat(lit("Direct Contract - "), $"fa.supplier_name")
      )
      .withColumn("payment_channel_clear",
        regexp_replace($"fa.payment_channel","adjustment.*,","")
      )
      .withColumn("coupon_code_split",
        when(size(split($"fa.coupon_code",",")) === 0,1)
          .otherwise(size(split($"fa.coupon_code",",")))
      )

      .select(
        to_date($"fa.booking_issue_date","dd/MM/yyyy").as("report_date"),
        coalesce(
          when($"fa.affiliate_id" =!= "" && $"fa.affiliate_id".isNotNull
          && $"fa.affiliate_id" =!= "N/A", regexp_replace($"fa.affiliate_id","-flight",""))
          when($"fa.corporate_type" =!= "" && $"fa.corporate_type".isNotNull
            && $"fa.corporate_type" =!= "N/A", $"fa.corporate_type")
          otherwise substring($"fa.locale",-2,2))
          .as("customer"),
        coalesce(when($"fa.business_model" === "N/A" || $"fa.business_model".isNull
          || $"fa.business_model" === "","CONSIGNMENT")
          .otherwise($"fa.business_model"))
          .as("business_model"),
        coalesce(when($"ful_id.wholesaler".isNull || $"ful_id.wholesaler" === "N/A"
          || $"ful_id.wholesaler" === "",$"supplier_name_concat")
          .otherwise($"ful_id.wholesaler"))
          .as("business_partner"),
        coalesce(when($"fa.product_category" === "N/A" || $"fa.product_category".isNull
          || $"fa.product_category" === "","BAGGAGE")
          .otherwise($"fa.product_category"))
          .as("product_category"),
        coalesce(split($"payment_channel_clear",",")(0),lit("None")).as("payment_channel"),
        $"fa.booking_id".as("booking_id"),
        coalesce(when($"fa.coupon_code" === "N/A" || $"fa.coupon_code".isNull
          || $"fa.coupon_code" === "",0)
          .otherwise($"coupon_code_split"))
          .as("coupon_code_result"),
        $"fa.quantity".as("quantity"),
        coalesce(when($"fa.contract_currency" === "IDR",$"fa.total_fare__contract_currency_")
          .otherwise($"fa.total_fare__contract_currency_" * $"contract_rate.conversion_rate"),lit(0))
          .as("total_fare__contract_currency__idr"),
        coalesce(when($"fa.collecting_currency" === "IDR",$"fa.commission_to_affiliate")
          .otherwise($"fa.commission_to_affiliate" * $"collecting_rate.conversion_rate"),lit(0))
          .as("commission_to_affiliate_idr"),
        coalesce(when($"fa.contract_currency" === "IDR",$"fa.commission")
          .otherwise($"fa.commission" * $"contract_rate.conversion_rate"),lit(0))
          .as("commission_idr"),
        coalesce(when($"fa.contract_currency" === "IDR",$"fa.total_fare___nta")
          .otherwise($"fa.total_fare___nta" * $"contract_rate.conversion_rate"),lit(0))
          .as("total_fare___nta_idr"),
        coalesce(when($"fa.collecting_currency" === "IDR",$"fa.discount_premium")
          .otherwise($"fa.discount_premium" * $"collecting_rate.conversion_rate"),lit(0))
          .as("discount_premium_idr"),
        coalesce(when($"fa.collecting_currency" === "IDR",$"fa.discount_wht")
          .otherwise($"fa.discount_wht" * $"collecting_rate.conversion_rate"),lit(0))
          .as("discount_wht_idr"),
        coalesce(when($"fa.collecting_currency" === "IDR",$"fa.unique_code")
          .otherwise($"fa.unique_code" * $"collecting_rate.conversion_rate"),lit(0))
          .as("unique_code_idr"),
        coalesce(when($"fa.collecting_currency" === "IDR",$"fa.coupon_value")
          .otherwise($"fa.coupon_value" * $"collecting_rate.conversion_rate"),lit(0))
          .as("coupon_value_idr"),
        coalesce(when($"fa.collecting_currency" === "IDR",$"fa.point_redemption")
          .otherwise($"fa.point_redemption" * $"collecting_rate.conversion_rate"),lit(0))
          .as("point_redemption_idr"),
        coalesce(when($"fa.collecting_currency" === "IDR",$"mdr_charges")
          .otherwise($"mdr_charges" * $"collecting_rate.conversion_rate"),lit(0))
          .as("mdr_charges_idr")
      )
  }
}