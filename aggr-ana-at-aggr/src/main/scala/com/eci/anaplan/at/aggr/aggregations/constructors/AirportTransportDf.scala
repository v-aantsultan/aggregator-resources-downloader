package com.eci.anaplan.at.aggr.aggregations.constructors

import com.eci.anaplan.at.aggr.services.S3SourceService
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class AirportTransportDf @Inject()(val sparkSession: SparkSession,
                                   s3SourceService: S3SourceService
                              ) {

  import sparkSession.implicits._

  def get: DataFrame = {
    s3SourceService.AirportTfDf.as("at")
      .withColumn("coupon_code_split",
        when(size(split($"coupon_id", ",")) === 0, 1)
          .otherwise(size(split($"coupon_id", ",")))
      )
      .withColumn("low_fulfillment_id",
        regexp_replace(
          lower($"at.fulfillment_id"), " ", ""
        )
      )
      .select(
        to_date($"at.booking_issue_date" + expr("INTERVAL 7 HOURS")).as("report_date"),
        substring($"at.locale", -2, 2).as("customer"),
        lit("consignment").as("business_model"),
        coalesce(
          when(
            $"low_fulfillment_id" === "bigbird" ||
            $"low_fulfillment_id" === "goldenbird" ||
            $"low_fulfillment_id" === "conxxe" ||
            $"low_fulfillment_id" === "kliaekspres" ||
            $"low_fulfillment_id" === "aries" ||
            $"low_fulfillment_id" === "ariesnon-id",
            $"at.fulfillment_id"
          ).otherwise(
            when($"low_fulfillment_id".like("%xtrans%"), lit("XTrans"))
            .when($"low_fulfillment_id".like("%railink%"), lit("Railink"))
            .otherwise(lit("Others"))
          )
        ).as("business_partner"),
        coalesce(
          when($"at.category" === "TRAIN", lit("TRAIN_AIRPORT_TRANSFER"))
          otherwise $"at.category"
        ).as("product_category"),
        coalesce(
          when($"payment_channel".isNull, "None")
            .otherwise(split($"at.payment_channel", "\\,").getItem(0))
        ).as("payment_channel"),
        coalesce(
          when($"at.coupon_id" === "N/A" || $"at.coupon_id".isNull || $"at.coupon_id" === "", 0)
            .otherwise($"coupon_code_split")
        ).as("coupon_code_result"),
        coalesce(
          when(lower($"at.transaction_type") === "sales", $"at.booking_id")
            .otherwise(null)
        ).as("no_of_tr"),
        $"at.booking_id".as("booking_id"),
        $"at.coupon_id".as("coupon_id"),
        $"at.quantity".as("quantity"),
        $"at.total_published_rate_contract_currency_idr".as("total_published_rate_contract_currency_idr"),
        $"at.gross_commission_idr".as("gross_commission_idr"),
        $"at.discount_premium_idr".as("discount_premium_idr"),
        $"at.discount_wht_idr".as("discount_wht_idr"),
        $"at.unique_code_idr".as("unique_code_idr"),
        $"at.coupon_value_idr".as("coupon_value_idr"),
        $"at.convenience_fee_idr".as("convenience_fee_idr"),
        $"at.vat_out_idr".as("vat_out_idr"),
        $"at.point_redemption_idr".as("point_redemption_idr"),
        $"at.rebook_cost_idr".as("rebook_cost_idr"),
        $"at.mdr_charges_idr".as("mdr_charges_idr")
      )
  }
}