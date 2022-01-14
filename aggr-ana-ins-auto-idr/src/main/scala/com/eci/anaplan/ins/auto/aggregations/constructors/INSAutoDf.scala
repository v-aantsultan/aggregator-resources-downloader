package com.eci.anaplan.ins.auto.aggregations.constructors

import com.eci.anaplan.ins.auto.services.S3SourceService
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{count, expr, to_date}
import org.apache.spark.sql.{DataFrame, SparkSession}
import javax.inject.{Inject, Singleton}

@Singleton
class INSAutoDf @Inject()(val sparkSession: SparkSession, s3SourceService: S3SourceService) {

  import sparkSession.implicits._

  def get: DataFrame = {
    s3SourceService.INSAutoDf
      .withColumn("count_bid",
          count($"`booking_id`").over(Window.partitionBy($"`booking_id`"))
      )

      .select(
        to_date($"`recognition_date`" + expr("INTERVAL 7 HOURS")).as("recognition_date"),
        $"`booking_issued_date`".as("booking_issued_date"),
        $"`booking_id`".as("booking_id"),
        $"count_bid".as("count_bid"),
        $"`product_type`".as("product_type"),
        $"`product_name`".as("product_name"),
        $"`insurance_plan`".as("insurance_plan"),
        $"`collecting_payment_entity`".as("collecting_payment_entity"),
        $"`payment_scope`".as("payment_scope"),
        $"`invoice_currency`".as("invoice_currency"),
        $"`total_actual_fare_paid_by_customer`".as("total_actual_fare_paid_by_customer"),
        $"`discount_or_premium`".as("discount_or_premium"),
        $"`discount_wht_expense`".as("discount_wht_expense"),
        $"`unique_code`".as("unique_code"),
        $"`coupon_value`".as("coupon_value"),
        $"`coupon_wht_expense`".as("coupon_wht_expense"),
        $"`point_redemption`".as("point_redemption"),
        $"`installment_request`".as("installment_request"),
        $"`total_wht_expense`".as("total_wht_expense"),
        $"`recognized_expense`".as("recognized_expense"),
        $"`provider_currency`".as("provider_currency"),
        $"`inventory_owner_entity`".as("inventory_owner_entity"),
        $"`total_fare_from_inventory_owner`".as("total_fare_from_inventory_owner"),
        $"`total_fare_from_provider_in_payment_currency`".as("total_fare_from_provider_in_payment_currency"),
        $"`fulfillment_id`".as("fulfillment_id"),
        $"`insurer_name`".as("insurer_name"),
        $"`business_model`".as("business_model"),
        $"`policy_id`".as("policy_id"),
        $"`insurance_expected_recognition_date`".as("insurance_expected_recognition_date"),
        $"`total_fare_from_provider`".as("total_fare_from_provider"),
        $"`total_fare_paid_to_provider`".as("total_fare_paid_to_provider"),
        $"`total_base_fare_from_commission`".as("total_base_fare_from_commission"),
        $"`insurance_commission`".as("insurance_commission"),
        $"`total_other_income`".as("total_other_income"),
        $"`collecting_payment_entity_insurance_commission_70%`".as("collecting_payment_entity_insurance_commission_70%"),
        $"`collecting_payment_entity_total_other_income_70%`".as("collecting_payment_entity_total_other_income_70%"),
        $"`inventory_owner_entity_insurance_commission_30%`".as("inventory_owner_entity_insurance_commission_30%"),
        $"`inventory_owner_entity_total_other_income_30%`".as("inventory_owner_entity_total_other_income_30%"),
        $"`is_interco`".as("is_interco"),
        $"`locale`".as("locale")
      )
  }
}