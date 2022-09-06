package com.eci.anaplan.bs.details.aggregations.constructors

import com.eci.anaplan.bs.details.services.S3SourceService
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class BusDetailsDf @Inject()(val sparkSession: SparkSession, s3SourceService: S3SourceService,
                             MDRChargesDf: MDRChargesDf, mappingBusinessPartnerDf: MappingBusinessPartnerDf) {

  import sparkSession.implicits._

  def get: DataFrame = {
    s3SourceService.BusDf.as("bus")
      .join(MDRChargesDf.get.as("mdr"),
        $"bus.booking_id" === $"mdr.booking_id"
        ,"left")
      .join(mappingBusinessPartnerDf.get.as("bp"),
        $"bus.vendor_name" === $"bp.vendor_name__sales_report_"
        ,"left")

      .withColumn("payment_channel_clear",
        regexp_replace(
          regexp_replace($"bus.payment_channel",
          "adjustment,",""),
          "adjustment_refund,","")
      )
      .withColumn("count_bid",
        count($"bus.booking_id").over(Window.partitionBy($"bus.booking_id"))
      )
      .withColumn("sum_customer_invoice_bid",
        sum($"bus.customer_invoice_final").over(Window.partitionBy($"bus.booking_id"))
      )
      .withColumn("final_mdr_charges_result",
        (($"sum_customer_invoice_bid" / $"mdr.expected_amount") * $"mdr.mdr_amount") / $"count_bid"
      )
      .withColumn("coupon_code_split",
        when(size(split($"bus.coupon_code",",")) === 0,1)
          .otherwise(size(split($"bus.coupon_code",",")))
      )

      .select(
        $"bus.formatted_date".as("report_date"),
        coalesce(when($"bus.affiliate_id" =!= "" && $"bus.affiliate_id".isNotNull
          && $"bus.affiliate_id" =!= "N/A", regexp_replace($"bus.affiliate_id","-bus",""))
          .otherwise(substring($"bus.locale",-2,2)))
          .as("customer"),
        coalesce(when($"bus.business_model" === "N/A" || $"bus.business_model".isNull
          || $"bus.business_model" === "","CONSIGNMENT")
          .otherwise($"bus.business_model"))
          .as("business_model"),
        coalesce(when($"bp.vendor_name__sales_report_".isNull,"BP Others")
          .otherwise($"bp.business_partner_mapping"))
          .as("business_partner"),
        lit("Bus").as("product"),
        when($"payment_channel_clear" === "0" || $"payment_channel_clear" === "" || $"payment_channel_clear".isNull , "None")
          .otherwise(trim(split($"payment_channel_clear",",")(0)))
          .as("payment_channel"),
        coalesce(when($"bus.transaction_type" === "Sales" || $"bus.transaction_type" === "sales",
            $"bus.booking_id"))
            .as("booking_id"),
          coalesce(when($"bus.coupon_code" === "N/A" || $"bus.coupon_code".isNull
            || $"bus.coupon_code" === "",0)
            .otherwise($"coupon_code_split"))
            .as("coupon_code_result"),
          $"bus.number_of_seats".as("number_of_seats"),
          $"bus.publish_rate_contract_currency".as("publish_rate_contract_currency"),
          coalesce($"bus.nett_commission" + $"bus.vat_out").as("commission_result"),
          $"bus.commission_expense_value".as("commission_expense_value"),
          $"bus.discount_premium".as("discount_premium"),
          $"bus.wht_discount".as("wht_discount"),
          $"bus.final_unique_code".as("final_unique_code"),
          $"bus.final_coupon_value".as("final_coupon_value"),
          $"bus.vat_out".as("vat_out"),
          $"bus.final_point_redemption".as("final_point_redemption"),
          $"final_mdr_charges_result".as("final_mdr_charges")
      )
  }
}