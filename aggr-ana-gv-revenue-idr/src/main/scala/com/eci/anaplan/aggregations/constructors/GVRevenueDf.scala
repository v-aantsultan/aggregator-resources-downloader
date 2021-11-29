package com.eci.anaplan.aggregations.constructors

import com.eci.anaplan.services.S3SourceService
import org.apache.spark.sql.functions.{expr, to_date}
import javax.inject.{Inject, Singleton}
import org.apache.spark.sql.{DataFrame, SparkSession}

// TODO: Update TestDataFrame1 and queries required
@Singleton
class GVRevenueDf @Inject()(val sparkSession: SparkSession, s3SourceService: S3SourceService) {

  import sparkSession.implicits._

  def get: DataFrame = {
    // TODO : Update this part of the code to get Domain data from S3
    s3SourceService.GVrevenueDf
      .select(
        $"`entity`".as("entity"),
        $"`transaction_id`".as("transaction_id"),
        $"`transaction_type`".as("transaction_type"),
        $"`product_type`".as("product_type"),
        $"`trip_type`".as("trip_type"),
        $"`gift_voucher_id`".as("gift_voucher_id"),
        $"`gift_voucher_currency`".as("gift_voucher_currency"),
        $"`gift_voucher_amount`".as("gift_voucher_amount"),
        $"`issued_date`".as("issued_date"),
        $"`planned_delivery_date`".as("planned_delivery_date"),
        $"`gift_voucher_expired_date`".as("gift_voucher_expired_date"),
        $"`partner_name`".as("partner_name"),
        $"`partner_id`".as("partner_id"),
        $"`business_model`".as("business_model"),
        $"`redeemed_booking_id`".as("redeemed_booking_id"),
        $"`redeemed_product_type`".as("redeemed_product_type"),
        $"`redeemed_trip_type`".as("redeemed_trip_type"),
        $"`redemption_date`".as("redemption_date"),
        $"`used_amount`".as("used_amount"),
        $"`revenue_amount`".as("revenue_amount"),
        $"`revenue_currency`".as("revenue_currency"),
        to_date($"`revenue_date`" + expr("INTERVAL 7 HOURS")).as("revenue_date"),
        $"`status`".as("status"),
        $"`selling_price`".as("selling_price"),
        $"`discount_amount`".as("discount_amount")
      )
  }
}
