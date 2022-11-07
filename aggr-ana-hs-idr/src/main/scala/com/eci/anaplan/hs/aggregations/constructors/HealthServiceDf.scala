package com.eci.anaplan.hs.aggregations.constructors

import com.eci.anaplan.hs.services.S3SourceService
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{count, expr, sum, to_date}
import org.apache.spark.sql.{DataFrame, SparkSession}
import javax.inject.{Inject, Singleton}

@Singleton
class HealthServiceDf @Inject()(val sparkSession: SparkSession, s3SourceService: S3SourceService) {

  import sparkSession.implicits._

  def get: DataFrame = {
    s3SourceService.HealthServiceDf
      .withColumn("count_bid",
        count($"`booking_id`").over(Window.partitionBy($"`booking_id`"))
      )
      .withColumn("sum_customer_invoice_incoming_fund_currency_bid",
        sum($"`customer_invoice_incoming_fund_currency`").over(Window.partitionBy($"`booking_id`"))
      )

      .select(
        $"`booking_issue_date`".as("booking_issue_date"),
        $"`trip_type`".as("trip_type"),
        $"`locale`".as("locale"),
        $"`booking_id`".as("booking_id"),
        $"count_bid",
        $"`fulfillment_id`".as("fulfillment_id"),
        $"`supplier_id`".as("supplier_id"),
        $"`supplier_name`".as("supplier_name"),
        $"`business_model`".as("business_model"),
        $"`contract_entity`".as("contract_entity"),
        $"`selling_entity`".as("selling_entity"),
        $"`selling_currency`".as("selling_currency"),
        $"`vendor_booking_id`".as("vendor_booking_id"),
        $"`product_id`".as("product_id"),
        $"`ticket_id`".as("ticket_id"),
        $"`product_name`".as("product_name"),
        $"`category`".as("category"),
        $"`product_country`".as("product_country"),
        $"`incoming_fund_entity`".as("incoming_fund_entity"),
        $"`payment_channel`".as("payment_channel"),
        $"`issuer_bank`".as("issuer_bank"),
        $"`installment_code`".as("installment_code"),
        $"`ticket_type`".as("ticket_type"),
        $"`number_of_tickets`".as("number_of_tickets"),
        $"`contract_currency`".as("contract_currency"),
        $"`published_rate_contract_currency`".as("published_rate_contract_currency"),
        $"`commission_percentage`".as("commission_percentage"),
        $"`recommended_price_contract_currency`".as("recommended_price_contract_currency"),
        $"`actual_gross_commission_contract_currency`".as("actual_gross_commission_contract_currency"),
        $"`gross_commission_to_collecting_contract_currency`".as("gross_commission_to_collecting_contract_currency"),
        $"`nta_contract_currency`".as("nta_contract_currency"),
        $"`incoming_fund_currency`".as("incoming_fund_currency"),
        $"`published_rate_incoming_fund_currency`".as("published_rate_incoming_fund_currency"),
        $"`discount_premium_incoming_fund_currency`".as("discount_premium_incoming_fund_currency"),
        $"`selling_price_incoming_fund_currency`".as("selling_price_incoming_fund_currency"),
        $"`recommended_price_incoming_fund_currency`".as("recommended_price_incoming_fund_currency"),
        $"`nta_incoming_fund_currency`".as("nta_incoming_fund_currency"),
        $"`actual_discount_premium_incoming_fund_currency`".as("actual_discount_premium_incoming_fund_currency"),
        $"`installment_fee`".as("installment_fee"),
        $"`transaction_fee`".as("transaction_fee"),
        $"`unique_code`".as("unique_code"),
        $"`coupon_value`".as("coupon_value"),
        $"`point_redemption`".as("point_redemption"),
        $"`gift_voucher`".as("gift_voucher"),
        $"`customer_invoice_incoming_fund_currency`".as("customer_invoice_incoming_fund_currency"),
        $"sum_customer_invoice_incoming_fund_currency_bid",
        $"`mdr_installment`".as("mdr_installment"),
        $"`vat_rate`".as("vat_rate"),
        $"`vat_out_base_contract_currency`".as("vat_out_base_contract_currency"),
        $"`vat_out_contract_currency`".as("vat_out_contract_currency"),
        $"`coupon_code`".as("coupon_code"),
        $"`coupon_description`".as("coupon_description"),
        $"`gift_voucher_code`".as("gift_voucher_code"),
        $"`plan_visit_date`".as("plan_visit_date"),
        $"`payment_type`".as("payment_type"),
        $"`payment_bank_name`".as("payment_bank_name"),
        $"`deposit_deduction_type`".as("deposit_deduction_type"),
        $"`primary_product_in_cross_selling`".as("primary_product_in_cross_selling"),
        $"`secondary_product_in_cross_selling`".as("secondary_product_in_cross_selling"),
        $"`non_refundable_date`".as("non_refundable_date"),
        $"`nett_commission_contract_currency`".as("nett_commission_contract_currency"),
        $"`premium_to_collecting_incoming_fund_currency`".as("premium_to_collecting_incoming_fund_currency"),
        $"`invoice_due_date`".as("invoice_due_date"),
        $"`is_easy_reservation`".as("is_easy_reservation"),
        $"`booking_expiry_date`".as("booking_expiry_date"),
        $"`discount_tax_expense`".as("discount_tax_expense"),
        $"`pot_booking_id`".as("pot_booking_id"),
        $"`mdr_charges`".as("mdr_charges")
      )
  }
}