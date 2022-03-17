package com.eci.anaplan.wm.aggregations.constructors

import com.eci.anaplan.wm.services.S3SourceService
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{count, expr, sum, to_date}
import org.apache.spark.sql.{DataFrame, SparkSession}
import javax.inject.{Inject, Singleton}

@Singleton
class WealthManagementDf @Inject()(val sparkSession: SparkSession, s3SourceService: S3SourceService) {

  import sparkSession.implicits._

  def get: DataFrame = {
    s3SourceService.WealthManagementDf
      .withColumn("count_bid",
        count($"`booking_id`").over(Window.partitionBy($"`booking_id`"))
      )
      .withColumn("sum_total_actual_fare_bid",
        sum($"`total_actual_fare_paid_by_customer`").over(Window.partitionBy($"`booking_id`"))
      )

      .select(
        to_date($"`booking_issued_date`" + expr("INTERVAL 7 HOURS")).as("booking_issued_date"),
        $"`booking_id`".as("booking_id"),
        $"count_bid",
        $"`investment_id`".as("investment_id"),
        $"`product_type`".as("product_type"),
        $"`product_name`".as("product_name"),
        $"`collecting_payment_entity`".as("collecting_payment_entity"),
        $"`payment_scope`".as("payment_scope"),
        $"`invoice_currency`".as("invoice_currency"),
        $"`total_actual_fare_paid_by_customer`".as("total_actual_fare_paid_by_customer"),
        $"sum_total_actual_fare_bid",
        $"`unique_code`".as("unique_code"),
        $"`coupon_value`".as("coupon_value"),
        $"`coupon_wht_expense`".as("coupon_wht_expense"),
        $"`point_redemption`".as("point_redemption"),
        $"`gift_voucher`".as("gift_voucher"),
        $"`total_wht_expense`".as("total_wht_expense"),
        $"`inventory_owner_entity`".as("inventory_owner_entity"),
        $"`total_fare_from_inventory_owner`".as("total_fare_from_inventory_owner"),
        $"`total_fare_from_provider_in_payment_currency`".as("total_fare_from_provider_in_payment_currency"),
        $"`fulfillment_id`".as("fulfillment_id"),
        $"`business_model`".as("business_model"),
        $"`transaction_type`".as("transaction_type"),
        $"`provider_transaction_id`".as("provider_transaction_id"),
        $"`topup_issuance_date`".as("topup_issuance_date"),
        $"`provider_currency`".as("provider_currency"),
        $"`topup_amount`".as("topup_amount"),
        $"`total_fare_paid_to_provider`".as("total_fare_paid_to_provider"),
        $"`total_base_fare_for_commission`".as("total_base_fare_for_commission"),
        $"`commission_revenue`".as("commission_revenue"),
        $"`collecting_payment_entity_commission_70_percentage`".as("collecting_payment_entity_commission_70_percentage"),
        $"`collecting_payment_entity_commission_30_percentage`".as("collecting_payment_entity_commission_30_percentage"),
        $"`is_interco`".as("is_interco"),
        $"`locale`".as("locale")
      )
  }
}