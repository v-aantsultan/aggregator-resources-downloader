package com.eci.anaplan.fa.summary.aggregations.constructors

import com.eci.anaplan.fa.summary.services.S3SourceService
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class FlightAncillariesSummaryDf @Inject()(val sparkSession: SparkSession,s3SourceService: S3SourceService) {

  import sparkSession.implicits._

  def get: DataFrame = {
    s3SourceService.FADf
      .withColumn("payment_channel_clear",
        regexp_replace($"payment_channel","adjustment.*,","")
      )
      .withColumn("coupon_code_split",
        when(size(split($"coupon_code",",")) === 0,1)
          .otherwise(size(split($"coupon_code",",")))
      )

      .select(
        to_date($"booking_issue_date","dd/MM/yyyy").as("report_date"),
        coalesce(
          when($"affiliate_id" =!= "" && $"affiliate_id".isNotNull
          && $"affiliate_id" =!= "N/A", regexp_replace($"affiliate_id","-flight",""))
          when($"corporate_type" =!= "" && $"corporate_type".isNotNull
            && $"corporate_type" =!= "N/A", $"corporate_type")
          otherwise substring($"locale",-2,2))
          .as("customer"),
        coalesce(when($"business_model" === "N/A" || $"business_model".isNull
          || $"business_model" === "","CONSIGNMENT")
          .otherwise($"business_model"))
          .as("business_model"),
        coalesce(when($"product_category" === "N/A" || $"product_category".isNull
          || $"product_category" === "","BAGGAGE")
          .otherwise($"product_category"))
          .as("product_category"),
        coalesce(split($"payment_channel_clear",",")(0),lit("None")).as("payment_channel"),
        $"booking_id".as("booking_id"),
        coalesce(when($"coupon_code" === "N/A" || $"coupon_code".isNull
          || $"coupon_code" === "",0)
          .otherwise($"coupon_code_split"))
          .as("coupon_code_result")
      )
  }
}