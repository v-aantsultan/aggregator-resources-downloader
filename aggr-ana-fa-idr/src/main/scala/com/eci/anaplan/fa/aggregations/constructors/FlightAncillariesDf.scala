package com.eci.anaplan.fa.aggregations.constructors

import com.eci.anaplan.fa.services.S3SourceService
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{count, sum, to_date}
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class FlightAncillariesDf @Inject()(val sparkSession: SparkSession, s3SourceService: S3SourceService) {

  import sparkSession.implicits._

  def get: DataFrame = {
    s3SourceService.FADf
      .withColumn("count_bid",
        count($"`booking_id`").over(Window.partitionBy($"`booking_id`"))
      )
      .withColumn("sum_customer_invoice_bid",
        sum($"`customer_invoice`").over(Window.partitionBy($"`booking_id`"))
      )

      .select(
        to_date($"`booking_issue_date`","dd/MM/yyyy").as("booking_issue_date"),
        $"`locale`".as("locale"),
        $"`product_type`".as("product_type"),
        $"`booking_id`".as("booking_id"),
        $"count_bid",
        $"`fulfillment_id`".as("fulfillment_id"),
        $"`supplier_id`".as("supplier_id"),
        $"`supplier_name`".as("supplier_name"),
        $"`business_model`".as("business_model"),
        $"`brand_codes`".as("brand_codes"),
        $"`provider_booking_id`".as("provider_booking_id"),
        $"`product_category`".as("product_category"),
        $"`product_id`".as("product_id"),
        $"`product_name`".as("product_name"),
        $"`contract_entity`".as("contract_entity"),
        $"`plan_visit_date`".as("plan_visit_date"),
        $"`non_refundable_date`".as("non_refundable_date"),
        $"`route`".as("route"),
        $"`product_countries`".as("product_countries"),
        $"`original_ticket_route`".as("original_ticket_route"),
        $"`collecting_entity`".as("collecting_entity"),
        $"`payment_channel`".as("payment_channel"),
        $"`issuer_bank`".as("issuer_bank"),
        $"`installment_code`".as("installment_code"),
        $"`quantity`".as("quantity"),
        $"`contract_currency`".as("contract_currency"),
        $"`total_fare__contract_currency_`".as("total_fare__contract_currency_"),
        $"`nta`".as("nta"),
        $"`total_fare___nta`".as("total_fare___nta"),
        $"`commission`".as("commission"),
        $"`issuance_fee`".as("issuance_fee"),
        $"`bta`".as("bta"),
        $"`collecting_currency`".as("collecting_currency"),
        $"`total_fare__collecting_currency_`".as("total_fare__collecting_currency_"),
        $"`discount_premium`".as("discount_premium"),
        $"`selling_price`".as("selling_price"),
        $"`installment_fee`".as("installment_fee"),
        $"`unique_code`".as("unique_code"),
        $"`coupon_value`".as("coupon_value"),
        $"`point_redemption`".as("point_redemption"),
        $"`customer_invoice`".as("customer_invoice"),
        $"sum_customer_invoice_bid",
        $"`mdr_charges`".as("mdr_charges"),
        $"`mdr_installment`".as("mdr_installment"),
        $"`coupon_code`".as("coupon_code"),
        $"`coupon_description`".as("coupon_description"),
        $"`reschedule_id`".as("reschedule_id"),
        $"`settlement_method`".as("settlement_method"),
        $"`primary_product`".as("primary_product"),
        $"`secondary_product`".as("secondary_product"),
        $"`affiliate_id`".as("affiliate_id"),
        $"`refunded_date`".as("refunded_date"),
        $"`refund_type`".as("refund_type"),
        $"`refund_amount_from_provider`".as("refund_amount_from_provider"),
        $"`corporate_id`".as("corporate_id"),
        $"`commission_to_affiliate`".as("commission_to_affiliate"),
        $"`rebook_cost`".as("rebook_cost"),
        $"`corporate_type`".as("corporate_type"),
        $"`customer_country`".as("customer_country"),
        $"`transaction_fee`".as("transaction_fee"),
        $"`gift_voucher_value`".as("gift_voucher_value"),
        $"`settlement_currency`".as("settlement_currency"),
        $"`total_fare__settlement_currency_`".as("total_fare__settlement_currency_"),
        $"`nta__settlement_currency_`".as("nta__settlement_currency_"),
        $"`total_fare___nta__settlement_currency_`".as("total_fare___nta__settlement_currency_"),
        $"`commission__settlement_currency_`".as("commission__settlement_currency_"),
        $"`issuance_fee__settlement_currency_`".as("issuance_fee__settlement_currency_"),
        $"`bta__settlement_currency_`".as("bta__settlement_currency_"),
        $"`discount_wht`".as("discount_wht"),
        $"`coupon_wht`".as("coupon_wht"),
        $"`flight_code`".as("flight_code"),
        $"`business_partner_id`".as("business_partner_id"),
        $"`selling_entity`".as("selling_entity")
      )
  }
}