package com.eci.anaplan.cd.idr.aggregations.constructors

import com.eci.anaplan.cd.idr.services.S3SourceService
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class ConnectivityDomesticDf @Inject()(val sparkSession: SparkSession, s3SourceService: S3SourceService) {

  import sparkSession.implicits._

  def get: DataFrame = {
    s3SourceService.ConnectivityDomesticDf
      .withColumn("count_bid",
          count($"`booking_id`").over(Window.partitionBy($"`booking_id`"))
      )
      .withColumn("sum_customer_invoice_bid",
          sum($"`customer_invoice`").over(Window.partitionBy($"`booking_id`"))
      )
      .select(
        $"`reporting_date`".as("reporting_date"),
        to_date($"`booking_issue_date`" + expr("INTERVAL 7 HOURS")).as("booking_issue_date"),
        $"`booking_id`".as("booking_id"),
        $"`product_type`".as("product_type"),
        $"`locale`".as("locale"),
        $"`locale_without_language`".as("locale_without_language"),
        $"`provider_booking_id`".as("provider_booking_id"),
        $"`provider_request_id`".as("provider_request_id"),
        $"`fulfillment_id`".as("fulfillment_id"),
        $"`product_id`".as("product_id"),
        $"`product_name`".as("product_name"),
        $"`business_model`".as("business_model"),
        $"`brand`".as("brand"),
        $"`product_category`".as("product_category"),
        $"`contract_entity`".as("contract_entity"),
        $"`selling_entity`".as("selling_entity"),
        $"`quantity`".as("quantity"),
        $"`contract_currency`".as("contract_currency"),
        $"`total_fare`".as("total_fare"),
        $"`admin_fee`".as("admin_fee"),
        $"`published_rate_in_contract_currency`".as("published_rate_in_contract_currency"),
        $"`net_to_agent`".as("net_to_agent"),
        $"`commission_revenue`".as("commission_revenue"),
        $"`selling_currency`".as("selling_currency"),
        $"`published_rate_in_selling_currency`".as("published_rate_in_selling_currency"),
        $"`discount_or_premium`".as("discount_or_premium"),
        $"`discount`".as("discount"),
        $"`premium`".as("premium"),
        $"`selling_price`".as("selling_price"),
        $"`transaction_fee`".as("transaction_fee"),
        $"`rebook_cost`".as("rebook_cost"),
        $"`refund_fee`".as("refund_fee"),
        $"`reschedule_fee`".as("reschedule_fee"),
        $"`installment_fee`".as("installment_fee"),
        $"`unique_code`".as("unique_code"),
        $"`total_coupon_value`".as("total_coupon_value"),
        $"`excluded_coupon_value_for_revenue`".as("excluded_coupon_value_for_revenue"),
        $"`included_coupon_value_for_revenue`".as("included_coupon_value_for_revenue"),
        $"`gift_voucher`".as("gift_voucher"),
        $"`point_redemption`".as("point_redemption"),
        $"`customer_invoice`".as("customer_invoice"),
        $"`coupon_code`".as("coupon_code"),
        $"`coupon_description`".as("coupon_description"),
        $"`payment_scope`".as("payment_scope"),
        $"`issuer_bank_name`".as("issuer_bank_name"),
        $"`installment_code`".as("installment_code"),
        $"`mdr_charges`".as("mdr_charges"),
        $"`mdr_installment`".as("mdr_installment"),
        $"`vat_rate`".as("vat_rate"),
        $"`tax_base_vat_out`".as("tax_base_vat_out"),
        $"`vat_out`".as("vat_out"),
        $"`tax_base_vat_in`".as("tax_base_vat_in"),
        $"`vat_in`".as("vat_in"),
        $"`gross_transaction_fee`".as("gross_transaction_fee"),
        $"`discount_wht`".as("discount_wht"),
        $"`coupon_wht`".as("coupon_wht"),
        $"`gross_revenue_in_usd`".as("gross_revenue_in_usd"),
        $"`discount_in_usd`".as("discount_in_usd"),
        $"`premium_in_usd`".as("premium_in_usd"),
        $"`total_coupon_in_usd`".as("total_coupon_in_usd"),
        $"`excluded_coupon_for_revenue_in_usd`".as("excluded_coupon_for_revenue_in_usd"),
        $"`included_coupon_for_revenue_in_usd`".as("included_coupon_for_revenue_in_usd"),
        $"`point_redemption_in_usd`".as("point_redemption_in_usd"),
        $"`unique_code_in_usd`".as("unique_code_in_usd"),
        $"`transaction_fee_in_usd`".as("transaction_fee_in_usd"),
        $"`rebook_cost_in_usd`".as("rebook_cost_in_usd"),
        $"`refund_fee_in_usd`".as("refund_fee_in_usd"),
        $"`reschedule_fee_in_usd`".as("reschedule_fee_in_usd"),
        $"`calculated_margin_in_usd`".as("calculated_margin_in_usd"),
        $"`margin_type`".as("margin_type"),
        $"`status`".as("status"),
        $"`refund_date`".as("refund_date"),
        $"`selling_to_usd_currency_rate`".as("selling_to_usd_currency_rate"),
        $"`contract_to_usd_currency_rate`".as("contract_to_usd_currency_rate"),
        $"`contract_to_selling_currency_rate`".as("contract_to_selling_currency_rate"),
        $"sum_customer_invoice_bid",
        $"count_bid"
      )
  }
}