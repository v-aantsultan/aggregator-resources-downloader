package com.eci.anaplan.bs.summary.aggregations.constructors

import com.eci.anaplan.bs.summary.services.S3SourceService
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class BusSummaryDf @Inject()(val sparkSession: SparkSession, s3SourceService: S3SourceService) {

  import sparkSession.implicits._

  def get: DataFrame = {
    s3SourceService.BusDf
      .withColumn("payment_channel_clear",
        regexp_replace(regexp_replace($"payment_channel",
          "adjustment,",""),
          "adjustment_refund,","")
      )
      .withColumn("coupon_code_split",
        when(size(split($"coupon_code",",")) === 0,1)
          .otherwise(size(split($"coupon_code",",")))
      )

      .select(
        $"formatted_date".as("report_date"),
        coalesce(when($"affiliate_id" =!= "" && $"affiliate_id".isNotNull
          && $"affiliate_id" =!= "N/A", regexp_replace($"affiliate_id","-bus",""))
          .otherwise(substring($"locale",-2,2)))
          .as("customer"),
        coalesce(when($"business_model" === "N/A" || $"business_model".isNull
          || $"business_model" === "","CONSIGNMENT")
          .otherwise($"business_model"))
          .as("business_model"),
        lit("Bus").as("product"),
        when($"payment_channel_clear" === "0" || $"payment_channel_clear" === "" || $"payment_channel_clear".isNull, lit("None"))
          .otherwise(trim(split($"payment_channel_clear",",")(0)))
          .as("payment_channel"),
        coalesce(when($"transaction_type" === "Sales" || $"transaction_type" === "sales",
          $"booking_id"))
          .as("booking_id"),
        coalesce(when($"coupon_code" === "N/A" || $"coupon_code".isNull
          || $"coupon_code" === "",0)
          .otherwise($"coupon_code_split"))
          .as("coupon_code_result")
      )
  }
}