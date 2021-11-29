package com.eci.anaplan.aggregations.constructors

import com.eci.anaplan.services.S3SourceService
import org.apache.spark.sql.functions.{expr, to_date}
import javax.inject.{Inject, Singleton}
import org.apache.spark.sql.{DataFrame, SparkSession}

// TODO: Update TestDataFrame1 and queries required
@Singleton
class GVSalesB2CDf @Inject()(val sparkSession: SparkSession, s3SourceService: S3SourceService) {

  import sparkSession.implicits._

  def get: DataFrame = {
    // TODO : Update this part of the code to get Domain data from S3
    s3SourceService.GVSalesB2CDf
      .select(
        $"`sales_delivery_id`".as("sales_delivery_id"),
        $"`entity`".as("entity"),
        $"`booking_id`".as("booking_id"),
        $"`product_type`".as("product_type"),
        $"`trip_type`".as("trip_type"),
        $"`invoice_amount`".as("invoice_amount"),
        $"`invoice_currency`".as("invoice_currency"),
        $"`gift_voucher_amount`".as("gift_voucher_amount"),
        $"`gift_voucher_currency`".as("gift_voucher_currency"),
        $"`gift_voucher_id`".as("gift_voucher_id"),
        to_date($"`issued_date`" + expr("INTERVAL 7 HOURS")).as("issued_date"),
        $"`planned_delivery_date`".as("planned_delivery_date"),
        $"`unique_code`".as("unique_code"),
        $"`coupon_value`".as("coupon_value"),
        $"`coupon_code`".as("coupon_code"),
        $"`discount_or_premium`".as("discount_or_premium"),
        $"`installment_fee_to_customer`".as("installment_fee_to_customer"),
        $"`installment_code`".as("installment_code"),
        $"`payment_mdr_fee_to_channel`".as("payment_mdr_fee_to_channel"),
        $"`installment_mdr_fee_to_bank`".as("installment_mdr_fee_to_bank"),
        $"`discount_wht`".as("discount_wht"),
        $"`coupon_wht`".as("coupon_wht"),
        $"`payment_channel_name`".as("payment_channel_name")
      )
  }
}