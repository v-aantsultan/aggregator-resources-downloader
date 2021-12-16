package com.eci.anaplan.ins.nonauto.aggregations.constructors

import com.eci.anaplan.ins.nonauto.services.INSNonAutoSource
import org.apache.spark.sql.functions.{expr, to_date}
import org.apache.spark.sql.{DataFrame, SparkSession}
import javax.inject.{Inject, Singleton}

@Singleton
class INSNonAutoDf @Inject()(val sparkSession: SparkSession, s3SourceService: INSNonAutoSource) {

  import sparkSession.implicits._

  def get: DataFrame = {
    s3SourceService.INSNonAutoDf
      .select(
        to_date($"`booking_issued_date`" + expr("INTERVAL 7 HOURS")).as("booking_issued_date"),
        $"`booking_id`".as("booking_id"),
        $"`product_type`".as("product_type"),
        $"`product_name`".as("product_name"),
        $"`insurance_plan`".as("insurance_plan"),
        $"`collecting_payment_entity`".as("collecting_payment_entity"),
        $"`payment_scope`".as("payment_scope"),
        $"`invoice_currency`".as("invoice_currency"),
        $"`total_actual_fare_paid_by_customer`".as("total_actual_fare_paid_by_customer"),
        $"`discount_or_premium`".as("discount_or_premium"),
        $"`unique_code`".as("unique_code"),
        $"`coupon_value`".as("coupon_value"),
        $"`point_redemption`".as("point_redemption"),
        $"`installment_request`".as("installment_request"),
        $"`inventory_owner_entity`".as("inventory_owner_entity"),
        $"`total_fare_from_inventory_owner`".as("total_fare_from_inventory_owner"),
        $"`total_fare_from_provider_in_payment_currency`".as("total_fare_from_provider_in_payment_currency"),
        $"`fulfillment_id`".as("fulfillment_id"),
        $"`business_model`".as("business_model"),
        $"`policy_id`".as("policy_id"),
        $"`insurance_issued_date`".as("insurance_issued_date"),
        $"`provider_currency`".as("provider_currency"),
        $"`total_fare_from_provider`".as("total_fare_from_provider"),
        $"`total_fare_paid_to_provider`".as("total_fare_paid_to_provider"),
        $"`total_base_fare_for_commission`".as("total_base_fare_for_commission"),
        $"`insurance_commission`".as("insurance_commission"),
        $"`total_other_income`".as("total_other_income"),
        $"`collecting_payment_entity_insurance_commission_70_percentage`"
          .as("collecting_payment_entity_insurance_commission_70_percentage"),
        $"`collecting_payment_entity_total_other_income_70_percentage`"
          .as("collecting_payment_entity_total_other_income_70_percentage"),
        $"`inventory_owner_entity_insurance_commission_30_percentage`"
          .as("inventory_owner_entity_insurance_commission_30_percentage"),
        $"`inventory_owner_entity_total_other_income_30_percentage`"
          .as("inventory_owner_entity_total_other_income_30_percentage"),
        $"`is_interco`".as("is_interco"),
        $"`locale`".as("locale"),
        $"`insurance_booking_item_id`".as("insurance_booking_item_id"),
        $"`discount_wht_expense`".as("discount_wht_expense"),
        $"`coupon_wht_expense`".as("coupon_wht_expense"),
        $"`total_wht_expense`".as("total_wht_expense"),
        $"`insurer_name`".as("insurer_name"),
        $"`booking_type`".as("booking_type")
      )
  }
}