package com.eci.anaplan.cr.details.aggregations.constructors

import com.eci.anaplan.cr.details.services.S3SourceService
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class CRDetailsDf @Inject()(val sparkSession: SparkSession, s3SourceService: S3SourceService) {

  import sparkSession.implicits._

  def get: DataFrame = {
    s3SourceService.CRDetailsDf.as("cr")

      .withColumn("payment_channel_clear",
        regexp_replace($"cr.payment_channel","adjustment.*,","")
      )
      .withColumn("coupon_code_split",
        when(size(split($"cr.coupon_code",",")) === 0,1)
          .otherwise(size(split($"cr.coupon_code",",")))
      )

      .select(
        to_date($"cr.booking_issue_date" + expr("INTERVAL 7 HOURS")).as("report_date"),
        substring($"cr.locale",-2,2).as("customer"),
        coalesce(when($"cr.business_model" === "N/A" || $"cr.business_model".isNull
          || $"cr.business_model" === "", "CONSIGNMENT")
          .otherwise($"cr.business_model"))
          .as("business_model"),
        coalesce(
          when($"cr.supplier_name" === "Moovby", "Moovby"),
          when($"cr.supplier_name" === "Jayamahe Easy Ride", "Jayamahe Easy Ride"),
          when($"cr.supplier_name" === "Smart Rent Car Driverless", "Smart Rent Car Driverless"),
          when($"cr.supplier_name" === "TRAC", "TRAC"),
          when($"cr.supplier_name" === "Wirasana Rent A Car", "Wirasana Rent A Car"),
          when($"cr.supplier_name" === "Salabi Car", "Salabi Car"),
          when($"cr.supplier_name" === "Moovby Driverless", "Moovby Driverless"),
          when($"cr.supplier_name" === "TRAVELDOTCOM", "TRAVELDOTCOM"),
          when($"cr.supplier_name" === "Nemob", "Nemob"),
          when($"cr.supplier_name" === "Romeo Car Rent", "Romeo Car Rent")
          .otherwise ("BP Others"))
          .as("business_partner"),
        coalesce(when($"cr.product_category" === "Empty String" || $"cr.product_category" === "Null"
          || $"cr.product_category" === "Blank" || $"cr.product_category".isNull || $"cr.product_category" === "", "VEHICLE")
          .otherwise($"cr.product_category"))
          .as("product_category"),
        split($"payment_channel_clear",",")(0).as("payment_channel"),
        coalesce(when($"cr.transaction_type" === "Sales", $"cr.booking_id"))
          .as("booking_id"),
        coalesce(when($"cr.coupon_code" === "N/A" || $"cr.coupon_code".isNull
          || $"cr.coupon_code" === "",0)
          .otherwise($"coupon_code_split"))
          .as("coupon_code_result"),
        $"cr.quantity".as("quantity"),
        $"cr.publish_rate_contract_currency_idr".as("publish_rate_contract_currency_idr"),
        $"cr.gross_commission_idr".as("gross_commission_idr"),
        $"cr.discount_premium_idr".as("discount_premium_idr"),
        $"cr.discount_wht_idr".as("discount_wht_idr"),
        $"cr.unique_code_idr".as("unique_code_idr"),
        $"cr.coupon_value_idr".as("coupon_value_idr"),
        $"cr.nta_idr".as("nta_idr"),
        $"cr.transaction_fee_idr".as("transaction_fee_idr"),
        $"cr.vat_out_idr".as("vat_out_idr"),
        $"cr.point_redemption_idr".as("point_redemption_idr"),
        $"cr.mdr_charges_idr".as("mdr_charges_idr")
      )
  }
}