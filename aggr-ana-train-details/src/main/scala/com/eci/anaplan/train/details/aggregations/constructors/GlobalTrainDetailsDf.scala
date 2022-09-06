package com.eci.anaplan.train.details.aggregations.constructors

import com.eci.anaplan.train.details.services.S3SourceService
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class GlobalTrainDetailsDf @Inject()(val sparkSession: SparkSession, s3SourceService: S3SourceService) {

  import sparkSession.implicits._

  def get: DataFrame = {
    s3SourceService.GlobalTrainDf
      .withColumn("payment_channel_clear",
        regexp_replace(regexp_replace($"payment_channel_name",
          "adjustment,",""),
          "adjustment_refund,","")
      )
      .withColumn("coupon_code_split",
        when(size(split($"coupon_code",",")) === 0,1)
          .otherwise(size(split($"coupon_code",",")))
      )

      .select(
        to_date($"sales_delivery_timestamp" + expr("INTERVAL 7 HOURS")).as("report_date"),
        substring($"locale",-2,2).as("customer"),
        when($"business_model" === "COMMISSION","Consignment")
          .otherwise($"business_model")
        .as("business_model"),
        $"fulfillment_id".as("business_partner"),
        when($"fulfillment_id" === "KAI_trinusa","KAI Ticket")
          .otherwise("Global Train")
          .as("product_category"),
        coalesce(trim(split($"payment_channel_clear",",")(0)),lit("None")).as("payment_channel"),
        $"pd_booking_id".as("booking_id"),
        when($"coupon_code" === "N/A" || $"coupon_code".isNull || $"coupon_code" === "",lit(0))
          .otherwise($"coupon_code_split")
        .as("coupon_code_result"),
        $"pax_quantity".as("pax_quantity"),
        $"published_rate_contracting_idr".as("gmv_result"),
        when($"business_model" === "COMMISSION",lit(0))
          .otherwise($"published_rate_contracting_idr")
        .as("gross_revenue_result"),
        when($"business_model" === "COMMISSION",$"provider_commission_idr")
          .otherwise(lit(0))
        .as("commission_result"),
        $"discount_or_premium_idr".as("discount_or_premium_idr"),
        $"wht_discount_idr".as("wht_discount_idr"),
        $"unique_code_idr".as("unique_code_idr"),
        $"coupon_amount_idr".as("coupon_amount_idr"),
        when($"business_model" === "COMMISSION",lit(0))
          .otherwise($"total_amount_idr" * -1)
        .as("nta_result"),
        $"transaction_fee_idr".as("transaction_fee_idr"),
        $"vat_out_idr".as("vat_out_idr"),
        $"point_redemption_idr".as("point_redemption_idr"),
        $"mdr_charges_idr".as("mdr_charges_idr")
    )
  }
}