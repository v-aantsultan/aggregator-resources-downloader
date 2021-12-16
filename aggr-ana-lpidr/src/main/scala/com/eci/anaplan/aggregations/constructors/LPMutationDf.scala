package com.eci.anaplan.aggregations.constructors

import com.eci.anaplan.services.LPMutationSource
import org.apache.spark.sql.functions.to_date
import javax.inject.{Inject, Singleton}
import org.apache.spark.sql.{DataFrame, SparkSession}

// TODO: Update TestDataFrame1 and queries required
@Singleton
class LPMutationDf @Inject()(val sparkSession: SparkSession, s3SourceService: LPMutationSource) {

  import sparkSession.implicits._

  def get: DataFrame = {

    // TODO : Update this part of the code to get Domain data from S3
    s3SourceService.LPMutationDf
        .select(
          $"`wallet_movement_id`".as("wallet_movement_id"),
          $"`wallet_content_id`".as("wallet_content_id"),
          $"`movement_type`".as("movement_type"),
          $"`movement_time`".as("movement_time"),
          to_date($"`posting_date`").as("posting_date"),
          $"`external_transaction_id`".as("external_transaction_id"),
          $"`original_transaction_id`".as("original_transaction_id"),
          $"`trip_type`".as("trip_type"),
          $"`booking_product_type`".as("booking_product_type"),
          $"`transaction_type`".as("transaction_type"),
          $"`customer_id`".as("customer_id"),
          $"`user_profile_id`".as("user_profile_id"),
          $"`cost_type_id`".as("cost_type_id"),
          $"`grant_product_type`".as("grant_product_type"),
          $"`grant_entity`".as("grant_entity"),
          $"`redeem_entity`".as("redeem_entity"),
          $"`transaction_currency`".as("transaction_currency"),
          $"`point_amount`".as("point_amount"),
          $"`point_conversion_rate`".as("point_conversion_rate"),
          $"`point_amount_in_transaction_currency`".as("point_amount_in_transaction_currency"),
          $"`selling_rate`".as("selling_rate"),
          $"`selling_currency`".as("selling_currency"),
          $"`earning_amount`".as("earning_amount"),
          $"`earning_currency`".as("earning_currency"),
          $"`non_refundable_date`".as("non_refundable_date"),
          $"`profit_center`".as("profit_center"),
          $"`cost_center`".as("cost_center"),
          $"`wc_original_transaction_id`".as("wc_original_transaction_id"),
          $"`business_partner`".as("business_partner"),
          $"`selling_conversion_rate`".as("selling_conversion_rate"),
          $"`total_selling_price`".as("total_selling_price"),
          $"`transaction_source`".as("transaction_source"),
          $"`unit_id`".as("unit_id"),
          $"`sales_issued_date`".as("sales_issued_date"),
          $"`sales_redemption_amount`".as("sales_redemption_amount"),
          $"`refunded_booking_id`".as("refunded_booking_id"),
          $"`refund_id`".as("refund_id"),
          $"`refund_provider_id`".as("refund_provider_id"),
          $"`business_partner_id`".as("business_partner_id"),
          $"`point_injection_id`".as("point_injection_id"),
          $"`multiplier_booking_id`".as("multiplier_booking_id"),
          $"`valid_from`".as("valid_from"),
          $"`valid_until`".as("valid_until"),
          $"`invoice_id`".as("invoice_id"),
          $"`wc_transaction_source`".as("wc_transaction_source"),
          $"`granted_point_wht_in_transaction_currency`".as("granted_point_wht_in_transaction_currency"),
          $"`refund_issued_date`".as("refund_issued_date"),
          $"`vat_selling_points`".as("vat_selling_points"),
          $"`instant_active_point`".as("instant_active_point")
        )
  }
}
