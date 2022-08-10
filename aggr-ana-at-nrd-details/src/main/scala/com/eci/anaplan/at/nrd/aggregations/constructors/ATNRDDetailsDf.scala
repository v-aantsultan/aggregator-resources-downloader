package com.eci.anaplan.at.nrd.aggregations.constructors

import com.eci.anaplan.at.nrd.services.S3SourceService
import org.apache.spark.sql.functions.{coalesce, expr, lit, lower, size, split, substring, to_date, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class ATNRDDetailsDf @Inject()(val sparkSession: SparkSession, s3SourceService: S3SourceService) {

  import sparkSession.implicits._

  def get: DataFrame = s3SourceService.ATNRDDf
    .withColumn("coupon_code_split",
      when(size(split($"coupon_id",",")) === 0,1)
        .otherwise(size(split($"coupon_id",",")))
    )
    .filter(lower($"is_refunded") === "false")
    .select(
      to_date($"non_refundable_date" + expr("INTERVAL 7 HOURS")).as("report_date"),
      substring($"locale",-2,2).as("customer"),
      lit("CONSIGNMENT").as("business_model"),
      coalesce(
        when($"fulfillment_id" === "Big Bird", "Big Bird"),
        when($"fulfillment_id" === "Golden Bird", "Golden Bird"),
        when($"fulfillment_id" === "Conxxe", "Conxxe"),
        when($"fulfillment_id" === "KLIA Ekspres", "KLIA Ekspres"),
        when($"fulfillment_id" === "ARIES", "ARIES"),
        when($"fulfillment_id" === "ARIES Non-ID", "ARIES Non-ID"),
        when($"fulfillment_id".like("%XTrans%"), "XTrans"),
        when($"fulfillment_id".like("%Railink%"), "Railink")
      .otherwise("Others"))
      .as("business_partner"),
      coalesce(
        when($"category" === "TRAIN", "TRAIN_AIRPORT_TRANSFER")
        .otherwise($"category"))
        .as("product_category"),
      coalesce(when($"transaction_type" === "Sales", $"booking_id"))
        .as("booking_id"),
      coalesce(when($"coupon_id" === "N/A" || $"coupon_id".isNull
        || $"coupon_id" === "",0)
        .otherwise($"coupon_code_split"))
        .as("coupon_code_result"),
      $"quantity".as("quantity"),
      $"total_published_rate_contract_currency_idr".as("total_published_rate_contract_currency_idr"),
      $"gross_commission_idr".as("gross_commission_idr"),
      $"discount_premium_idr".as("discount_premium_idr"),
      $"discount_wht_idr".as("discount_wht_idr"),
      $"unique_code_idr".as("unique_code_idr"),
      $"coupon_value_idr".as("coupon_value_idr"),
      $"convenience_fee_idr".as("convenience_fee_idr"),
      $"vat_out_idr".as("vat_out_idr"),
      $"point_redemption_idr".as("point_redemption_idr"),
      $"rebook_cost_idr".as("rebook_cost_idr")
  )
}