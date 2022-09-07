package com.eci.anaplan.ci.details.aggregations.constructors

import com.eci.anaplan.ci.details.services.S3SourceService
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class ConnectivityInternationalDf @Inject()(val sparkSession: SparkSession,
                                            s3SourceService: S3SourceService) {

  import sparkSession.implicits._

  def get: DataFrame = {
    s3SourceService.ConnectivityInternationalDf
      .withColumn("coupon_code_split",
        when(size(split($"coupon_code", ",")) === 0, 1)
          .otherwise(size(split($"coupon_code", ",")))
      )
      .withColumn("payment_channel_clear",
        regexp_replace(regexp_replace($"payment_channel_name",
          "adjustment,",""),
          "adjustment_refund,","")
      )

      .select(
        to_date($"booking_issue_date" + expr("INTERVAL 7 HOURS")).as("report_date"),
        substring($"locale", -2, 2).as("customer"),
        when(upper($"business_model") === "COMMISSION", "CONSIGNMENT")
          .otherwise($"business_model")
          .as("business_model"),
        when($"fulfillment_id" === "" || $"fulfillment_id".isNull, "3rd Party")
          .otherwise($"fulfillment_id")
          .as("business_partner"),
        $"product_category".as("product_category"),
        coalesce(trim(split($"payment_channel_clear",",")(0)),lit("None")).as("payment_channel"),
        when($"coupon_code" === "N/A" || $"coupon_code".isNull || $"coupon_code" === "", lit(0))
          .otherwise($"coupon_code_split")
          .as("coupon_code_result"),
        $"discount_or_premium_idr".as("discount_or_premium_idr"),
        $"discount_wht_collect_to_idr".as("discount_wht_collect_to_idr"),
        $"coupon_value_idr".as("coupon_value_idr"),
        $"nta_idr".as("nta_idr"),
        $"quantity".as("quantity"),
        $"booking_id".as("booking_id"),
        $"vat_out_idr".as("vat_out_idr"),
        $"unique_code_idr".as("unique_code_idr"),
        $"gross_commission_idr".as("gross_commission_idr"),
        $"point_redemption_idr".as("point_redemption_idr"),
        $"total_published_rate_idr".as("total_published_rate_idr"),
        $"mdr_charges_idr".as("mdr_charges_idr")
      )
  }
}