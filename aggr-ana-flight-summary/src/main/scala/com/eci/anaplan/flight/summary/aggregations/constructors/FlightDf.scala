package com.eci.anaplan.flight.summary.aggregations.constructors

import com.eci.anaplan.flight.idr.aggregations.joiners.FlFinalJoiner
import org.apache.spark.sql.functions._
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
      .select(
        $"`booking_issue_date`".as("report_date"),
        $"`locale`".as("locale"),
        $"`affiliate_id`".as("affiliate_id"),
        $"`corporate_type`".as("corporate_type"),
        $"`business_model`".as("business_model"),
        $"`product_type`".as("product"),
        when($"domestic_international".like("Dom%"), lit("Domestic"))
          .otherwise(lit("International"))
          .as("product_category"),
        coalesce(trim(split($"payment_channel_clear",",")(0)), lit("None")).as("payment_channel"),
        $"`booking_id`".as("booking_id"),
        when($"coupon_code" === "N/A" || $"coupon_code".isNull || $"coupon_code" === "", 0)
          .otherwise($"coupon_code_split")
          .as("coupon_code_result")
      )
  }
}