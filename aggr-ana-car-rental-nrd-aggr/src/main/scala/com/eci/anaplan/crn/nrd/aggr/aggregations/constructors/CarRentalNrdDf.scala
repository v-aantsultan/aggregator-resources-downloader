package com.eci.anaplan.crn.nrd.aggr.aggregations.constructors

import com.eci.anaplan.crn.nrd.aggr.services.S3SourceService
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class CarRentalNrdDf @Inject()(val sparkSession: SparkSession,
                               s3SourceService: S3SourceService
                              ) {

  import sparkSession.implicits._

  def get: DataFrame = {
    s3SourceService.CarRentalNrdDf
      .filter(lower($"is_refunded") === "false")

      .withColumn("coupon_code_split",
        when(size(split($"coupon_code",",")) === 0,lit(1))
          .otherwise(size(split($"coupon_code",",")))
      )
      .withColumn("supplier_name_fixed",
        regexp_replace($"supplier_name", " ", "")
      )

      .select(
        to_date($"non_refundable_date" + expr("INTERVAL 7 HOURS")).as("report_date"),
        substring($"locale", -2, 2).as("customer"),
        when($"business_model" === "Empty String" || $"business_model" === "Null"
          || $"business_model" === "Blank" || $"business_model".isNull || $"business_model" === "", lit("CONSIGNMENT"))
          .otherwise($"business_model").as("business_model"),
        when(lower($"supplier_name_fixed")
          .isin(
            "moovby",
            "jayamaheeasyride",
            "smartrentcardriverless",
            "trac",
            "wirasanarentacar",
            "salabicar",
            "moovbydriverless",
            "traveldotcom",
            "nemob",
            "romeocarrent"
          ),$"supplier_name")
          .otherwise(lit("BP Others"))
          .as("business_partner"),
        when($"product_category" === "Empty String" || $"product_category" === "Null" || $"product_category" === "Blank"
          || $"product_category".isNull || $"product_category" === "", lit("VEHICLE"))
          .otherwise($"product_category")
          .as("product_category"),
        when($"transaction_type" === "Sales", $"booking_id")
          .otherwise(lit(null))
          .as("booking_id_fix"),
        $"`booking_id`".as("booking_id"),
        $"`quantity`".as("quantity"),
        when($"coupon_code" === "N/A" || $"coupon_code".isNull || $"coupon_code" === "", lit(0))
          .otherwise($"coupon_code_split")
          .as("coupon_code_result"),
        $"`publish_rate_contract_currency_idr`".as("publish_rate_contract_currency_idr"),
        $"`gross_commission_idr`".as("gross_commission_idr"),
        $"`discount_premium_idr`".as("discount_premium_idr"),
        $"`discount_wht_idr`".as("discount_wht_idr"),
        $"`unique_code_idr`".as("unique_code_idr"),
        $"`coupon_value_idr`".as("coupon_value_idr"),
        $"`nta_idr`".as("nta_idr"),
        $"`transaction_fee_idr`".as("transaction_fee_idr"),
        $"`vat_out_idr`".as("vat_out_idr"),
        $"`point_redemption_idr`".as("point_redemption_idr")
      )
  }
}