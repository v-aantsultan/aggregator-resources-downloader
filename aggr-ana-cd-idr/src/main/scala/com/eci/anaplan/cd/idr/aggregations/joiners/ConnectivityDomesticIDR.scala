package com.eci.anaplan.cd.idr.aggregations.joiners

import com.eci.anaplan.cd.idr.aggregations.constructors._
import com.eci.anaplan.cd.idr.aggregations.constructors.{ConnectivityDomesticDf, ExchangeRateDf, MDRChargesDf}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class ConnectivityDomesticIDR @Inject()(spark: SparkSession,
                                        connectivityDomesticDf: ConnectivityDomesticDf,
                                        ExchangeRateDf: ExchangeRateDf,
                                        MDRChargesDf: MDRChargesDf) {

  private def joinDataFrames: DataFrame = {

    import spark.implicits._

    connectivityDomesticDf.get.as("cd")
      .join(ExchangeRateDf.get.as("invoice_rate"),
        $"cd.selling_currency" === $"invoice_rate.from_currency"
          && $"cd.booking_issue_date" === $"invoice_rate.conversion_date"
        , "left")
      .join(MDRChargesDf.get.as("mdr"),
        $"cd.booking_id" === $"mdr.booking_id"
        , "left")
      .withColumn("mdr_amount_prorate",
        (($"cd.sum_customer_invoice_bid" / $"mdr.expected_amount") * $"mdr.mdr_amount") / $"cd.count_bid"
      )
      .select(
        $"cd.reporting_date",
        $"cd.booking_issue_date",
        $"cd.booking_id",
        $"cd.product_type",
        $"cd.locale",
        $"cd.locale_without_language",
        $"cd.provider_booking_id",
        $"cd.provider_request_id",
        $"cd.fulfillment_id",
        $"cd.product_id",
        $"cd.product_name",
        $"cd.business_model",
        $"cd.brand",
        $"cd.product_category",
        $"cd.contract_entity",
        $"cd.selling_entity",
        $"cd.quantity",
        $"cd.contract_currency",
        $"cd.total_fare",
        $"cd.admin_fee",
        $"cd.published_rate_in_contract_currency",
        $"cd.net_to_agent",
        $"cd.commission_revenue",
        $"cd.selling_currency",
        $"cd.published_rate_in_selling_currency",
        $"cd.discount_or_premium",
        $"cd.discount",
        $"cd.premium",
        $"cd.selling_price",
        $"cd.transaction_fee",
        $"cd.rebook_cost",
        $"cd.refund_fee",
        $"cd.reschedule_fee",
        $"cd.installment_fee",
        $"cd.unique_code",
        $"cd.total_coupon_value",
        $"cd.excluded_coupon_value_for_revenue",
        $"cd.included_coupon_value_for_revenue",
        $"cd.gift_voucher",
        $"cd.point_redemption",
        $"cd.customer_invoice",
        $"cd.coupon_code",
        $"cd.coupon_description",
        $"cd.payment_scope",
        $"cd.issuer_bank_name",
        $"cd.installment_code",
        $"cd.mdr_charges",
        $"cd.mdr_installment",
        $"cd.vat_rate",
        $"cd.tax_base_vat_out",
        $"cd.vat_out",
        $"cd.tax_base_vat_in",
        $"cd.vat_in",
        $"cd.gross_transaction_fee",
        $"cd.discount_wht",
        $"cd.coupon_wht",
        $"cd.gross_revenue_in_usd",
        $"cd.discount_in_usd",
        $"cd.premium_in_usd",
        $"cd.total_coupon_in_usd",
        $"cd.excluded_coupon_for_revenue_in_usd",
        $"cd.included_coupon_for_revenue_in_usd",
        $"cd.point_redemption_in_usd",
        $"cd.unique_code_in_usd",
        $"cd.transaction_fee_in_usd",
        $"cd.rebook_cost_in_usd",
        $"cd.refund_fee_in_usd",
        $"cd.reschedule_fee_in_usd",
        $"cd.calculated_margin_in_usd",
        $"cd.margin_type",
        $"cd.status",
        $"cd.refund_date",
        $"cd.selling_to_usd_currency_rate",
        $"cd.contract_to_usd_currency_rate",
        $"cd.contract_to_selling_currency_rate",
        coalesce($"mdr_amount_prorate", lit(0)).as("mdr_amount_prorate"),
        coalesce(when($"cd.selling_currency" === "IDR", $"mdr_amount_prorate")
          .otherwise($"mdr_amount_prorate" * $"invoice_rate.conversion_rate"), lit(0))
          .as("mdr_amount_prorate_idr")
      )
  }

  def joinWithColumn(): DataFrame =
    joinDataFrames
}