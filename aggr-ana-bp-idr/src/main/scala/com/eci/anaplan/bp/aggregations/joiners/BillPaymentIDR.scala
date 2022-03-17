package com.eci.anaplan.bp.aggregations.joiners

import com.eci.anaplan.bp.aggregations.constructors.{BillPaymentDf, ExchangeRateDf, MDRChargesDf}
import org.apache.spark.sql.functions.{coalesce, lit, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class BillPaymentIDR @Inject()(spark: SparkSession,
                               BillPaymentDf: BillPaymentDf,
                               ExchangeRateDf: ExchangeRateDf,
                               MDRChargesDf: MDRChargesDf) {

  private def joinDataFrames: DataFrame = {

    import spark.implicits._

    BillPaymentDf.get.as("bp")
      .join(ExchangeRateDf.get.as("invoice_rate"),
        $"bp.selling_currency" === $"invoice_rate.from_currency"
          && $"bp.booking_issue_date" === $"invoice_rate.conversion_date"
        ,"left")
      .join(MDRChargesDf.get.as("mdr"),
        $"bp.booking_id" === $"mdr.booking_id"
        ,"left")

      .withColumn("mdr_amount_prorate",
        (($"bp.sum_customer_invoice_bid" / $"mdr.expected_amount") * $"mdr.mdr_amount") / $"bp.count_bid"
      )

      .select(
        $"bp.reporting_date",
        $"bp.booking_issue_date",
        $"bp.booking_id",
        $"bp.product_type",
        $"bp.locale",
        $"bp.locale_without_language",
        $"bp.provider_booking_id",
        $"bp.fulfillment_id",
        $"bp.product_id",
        $"bp.product_name",
        $"bp.business_model",
        $"bp.operator",
        $"bp.product_category",
        $"bp.contract_entity",
        $"bp.selling_entity",
        $"bp.contract_currency",
        $"bp.total_fare",
        $"bp.admin_fee",
        $"bp.published_rate_in_contract_currency",
        $"bp.net_to_agent",
        $"bp.commission_revenue",
        $"bp.selling_currency",
        $"bp.published_rate_in_selling_currency",
        $"bp.discount_or_premium",
        $"bp.discount",
        $"bp.premium",
        $"bp.selling_price",
        $"bp.transaction_fee",
        $"bp.rebook_cost",
        $"bp.refund_fee",
        $"bp.reschedule_fee",
        $"bp.installment_fee",
        $"bp.unique_code",
        $"bp.total_coupon_value",
        $"bp.excluded_coupon_value_for_revenue",
        $"bp.included_coupon_value_for_revenue",
        $"bp.gift_voucher",
        $"bp.point_redemption",
        $"bp.customer_invoice",
        $"bp.coupon_code",
        $"bp.coupon_description",
        $"bp.payment_scope",
        $"bp.issuer_bank_name",
        $"bp.installment_code",
        $"bp.mdr_charges",
        $"bp.mdr_installment",
        $"bp.vat_rate",
        $"bp.tax_base_vat_out",
        $"bp.vat_out",
        $"bp.tax_base_vat_in",
        $"bp.vat_in",
        $"bp.gross_transaction_fee",
        $"bp.discount_wht",
        $"bp.coupon_wht",
        $"bp.gross_revenue_in_usd",
        $"bp.discount_in_usd",
        $"bp.premium_in_usd",
        $"bp.total_coupon_in_usd",
        $"bp.excluded_coupon_for_revenue_in_usd",
        $"bp.included_coupon_for_revenue_in_usd",
        $"bp.point_redemption_in_usd",
        $"bp.unique_code_in_usd",
        $"bp.transaction_fee_in_usd",
        $"bp.rebook_cost_in_usd",
        $"bp.refund_fee_in_usd",
        $"bp.reschedule_fee_in_usd",
        $"bp.calculated_margin_in_usd",
        $"bp.margin_type",
        $"bp.status",
        $"bp.refund_date",
        $"bp.selling_to_usd_currency_rate",
        $"bp.contract_to_usd_currency_rate",
        $"bp.contract_to_selling_currency_rate",
        $"bp.commission_scheme",
        coalesce($"mdr_amount_prorate",lit(0)).as("mdr_amount_prorate"),
        coalesce(when($"bp.selling_currency" === "IDR",$"mdr_amount_prorate")
          .otherwise($"mdr_amount_prorate" * $"invoice_rate.conversion_rate"),lit(0))
          .as("mdr_amount_prorate_idr")
      )
  }

  def joinWithColumn(): DataFrame =
    joinDataFrames
}