package com.eci.anaplan.flight.details.aggregations.constructors

import com.eci.anaplan.flight.idr.aggregations.joiners.FlFinalJoiner
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class FlightDf @Inject()(val sparkSession: SparkSession, flFinalJoiner: FlFinalJoiner) {

  import sparkSession.implicits._

  def get: DataFrame = {
    flFinalJoiner.get()
      .withColumn("payment_channel_clear",
        regexp_replace(
          regexp_replace($"payment_scope",
            "adjustment,",""),
          "adjustment_refund,","")
      )
      .withColumn("coupon_code_split",
        when(size(split($"coupon_code", ",")) === 0, 1)
          .otherwise(size(split($"coupon_code", ",")))
      )
      .withColumn("discount_ready",
        when($"discount_premium_idr" <= 0 && $"non_connecting_discount_premium_idr" >= 0 && $"package_discount_premium_idr" >= 0,
          $"discount_premium_idr" + $"discount_wht_idr")
          .when($"discount_premium_idr" <= 0 && $"non_connecting_discount_premium_idr" <= 0 && $"package_discount_premium_idr" >= 0,
            $"discount_premium_idr" + $"non_connecting_discount_premium_idr" + $"discount_wht_idr")
          .when($"discount_premium_idr" <= 0 && $"non_connecting_discount_premium_idr" <= 0 && $"package_discount_premium_idr" <= 0,
            $"discount_premium_idr" + $"non_connecting_discount_premium_idr" + $"package_discount_premium_idr" + $"discount_wht_idr")
          .when($"discount_premium_idr" >= 0 && $"non_connecting_discount_premium_idr" <= 0 && $"package_discount_premium_idr" <= 0,
            $"non_connecting_discount_premium_idr" + $"package_discount_premium_idr" + $"discount_wht_idr")
          .when($"discount_premium_idr" >= 0 && $"non_connecting_discount_premium_idr" <= 0 && $"package_discount_premium_idr" >= 0,
            $"non_connecting_discount_premium_idr" + $"discount_wht_idr")
          .when($"discount_premium_idr" >= 0 && $"non_connecting_discount_premium_idr" >= 0 && $"package_discount_premium_idr" <= 0,
            $"package_discount_premium_idr" + $"discount_wht_idr")
          .otherwise(lit(0))
      )
      .withColumn("premium_ready",
        when($"discount_premium_idr" >= 0 && $"non_connecting_discount_premium_idr" <= 0 && $"package_discount_premium_idr" <= 0,
          $"discount_premium_idr")
          .when($"discount_premium_idr" >= 0 && $"non_connecting_discount_premium_idr" >= 0 && $"package_discount_premium_idr" <= 0,
            $"discount_premium_idr" + $"non_connecting_discount_premium_idr")
          .when($"discount_premium_idr" >= 0 && $"non_connecting_discount_premium_idr" >= 0 && $"package_discount_premium_idr" >= 0,
            $"discount_premium_idr" + $"non_connecting_discount_premium_idr" + $"package_discount_premium_idr")
          .when($"discount_premium_idr" <= 0 && $"non_connecting_discount_premium_idr" >= 0 && $"package_discount_premium_idr" >= 0,
            $"non_connecting_discount_premium_idr" + $"package_discount_premium_idr")
          .when($"discount_premium_idr" <= 0 && $"non_connecting_discount_premium_idr" >= 0 && $"package_discount_premium_idr" <= 0,
            $"non_connecting_discount_premium_idr")
          .when($"discount_premium_idr" <= 0 && $"non_connecting_discount_premium_idr" <= 0 && $"package_discount_premium_idr" >= 0,
            $"package_discount_premium_idr")
          .otherwise(lit(0))
      )

      .select(
        $"booking_issue_date".as("report_date"),
        $"`locale`".as("locale"),
        $"`affiliate_id`".as("affiliate_id"),
        $"fulfillment_id",
        $"`corporate_type`".as("corporate_type"),
        $"`business_model`".as("business_model"),
        when($"wholesaler".isin("N/A","") || $"wholesaler".isNull, lit("Direct Airlines"))
          .otherwise($"wholesaler")
          .as("business_partner"),
        $"`product_type`".as("product"),
        when($"domestic_international".like("Dom%"), lit("Domestic"))
          .otherwise(lit("International"))
          .as("product_category"),
        coalesce(trim(split($"payment_channel_clear",",")(0)), lit("None")).as("payment_channel"),
        $"`airline_id`".as("airline_id"),
        $"`booking_id`".as("booking_id"),
        when($"coupon_code" === "N/A" || $"coupon_code".isNull || $"coupon_code" === "", lit(0))
          .otherwise($"coupon_code_split")
          .as("coupon_code_result"),
        $"`total_segments_pax__exclude_infant_`".as("total_segments_pax__exclude_infant_"),
        when($"point_redemption_idr".isin("0",""), lit(null))
          .otherwise($"point_redemption_idr")
          .as("is_point_redemption_idr"),
          when($"business_model" === "CONSIGNMENT", $"total_fare__contract_currency__idr")
            .otherwise($"total_fare__contract_currency__idr" + $"premium_ready")
            .as("gmv_ready"),
        ($"new_gross_commission_idr" - $"no_hold_booking_cost_idr").as("commission_ready"),
        $"discount_ready",
        $"premium_ready",
        $"`unique_code_idr`".as("unique_code_idr"),
        $"`coupon_value_idr`".as("coupon_value_idr"),
        $"`total_fare__contract_currency__idr`".as("total_fare__contract_currency__idr"),
        $"`transaction_fee_idr`".as("transaction_fee_idr"),
        $"`point_redemption_idr`".as("point_redemption_idr"),
        $"`issuance_fee_idr`".as("issuance_fee_idr"),
        $"`commission_to_affiliate_idr`".as("commission_to_affiliate_idr"),
        $"`basic_incentive_idr`".as("basic_incentive_idr"),
        $"`rebook_cost_idr`".as("rebook_cost_idr"),
        $"`traveloka_reschedule_fee__contract_currency__idr`".as("traveloka_reschedule_fee__contract_currency__idr"),
        lit(0).as("premium_reschedule_fee_idr"),
        lit(0).as("refund_fee_idr"),
        $"traveloka_reschedule_fee__contract_currency__idr".as("total_reschedule_fee_ready"),
        $"`mdr_charges_idr`".as("mdr_charges_idr")
      )
  }
}