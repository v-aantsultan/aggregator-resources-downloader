package com.eci.anaplan.aggregations.constructors

import com.eci.anaplan.services.GVB2CSource
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{count, expr, to_date, sum}
import javax.inject.{Inject, Singleton}
import org.apache.spark.sql.{DataFrame, SparkSession}

@Singleton
class GVSalesB2CDf @Inject()(val sparkSession: SparkSession, s3SourceService: GVB2CSource) {

  import sparkSession.implicits._

  def get: DataFrame = {
    s3SourceService.GVSalesB2CDf
      .withColumn("count_bid",
        count($"`booking_id`").over(Window.partitionBy($"`booking_id`"))
      )
      .withColumn("sum_invoice_bid",
        sum($"`invoice_amount`").over(Window.partitionBy($"`booking_id`"))
      )

      .select(
        $"`sales_delivery_id`".as("sales_delivery_id"),
        $"`entity`".as("entity"),
        $"`booking_id`".as("booking_id"),
        $"count_bid".as("count_bid"),
        $"`product_type`".as("product_type"),
        $"`trip_type`".as("trip_type"),
        $"`invoice_amount`".as("invoice_amount"),
        $"sum_invoice_bid",
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