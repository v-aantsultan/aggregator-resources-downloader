package com.eci.anaplan.hs.aggregations.joiners

import com.eci.anaplan.hs.aggregations.constructors.{HealthServiceDf, MDRChargesDf}
import org.apache.spark.sql.functions.{coalesce, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class HealthServiceIDR @Inject()(spark: SparkSession,
                                 HealthServiceDf: HealthServiceDf,
                                 MDRChargesDf: MDRChargesDf) {

  private def joinDataFrames: DataFrame = {

    import spark.implicits._

    HealthServiceDf.get.as("hs")
      .join(MDRChargesDf.get.as("mdr"),
        $"hs.booking_id" === $"mdr.booking_id"
        ,"left")

      .withColumn("mdr_charges_prorate",
        (($"hs.sum_customer_invoice_incoming_fund_currency_bid" / $"mdr.expected_amount") * $"mdr.mdr_amount") / $"hs.count_bid"
      )

      .select(
        $"hs.booking_issue_date",
        $"hs.trip_type",
        $"hs.locale",
        $"hs.booking_id",
        $"hs.fulfillment_id",
        $"hs.supplier_id",
        $"hs.supplier_name",
        $"hs.business_model",
        $"hs.contract_entity",
        $"hs.selling_entity",
        $"hs.selling_currency",
        $"hs.vendor_booking_id",
        $"hs.product_id",
        $"hs.ticket_id",
        $"hs.product_name",
        $"hs.category",
        $"hs.product_country",
        $"hs.incoming_fund_entity",
        $"hs.payment_channel",
        $"hs.issuer_bank",
        $"hs.installment_code",
        $"hs.ticket_type",
        $"hs.number_of_tickets",
        $"hs.contract_currency",
        $"hs.published_rate_contract_currency",
        $"hs.commission_percentage",
        $"hs.recommended_price_contract_currency",
        $"hs.actual_gross_commission_contract_currency",
        $"hs.gross_commission_to_collecting_contract_currency",
        $"hs.nta_contract_currency",
        $"hs.incoming_fund_currency",
        $"hs.published_rate_incoming_fund_currency",
        $"hs.discount_premium_incoming_fund_currency",
        $"hs.selling_price_incoming_fund_currency",
        $"hs.recommended_price_incoming_fund_currency",
        $"hs.nta_incoming_fund_currency",
        $"hs.actual_discount_premium_incoming_fund_currency",
        $"hs.installment_fee",
        $"hs.transaction_fee",
        $"hs.unique_code",
        $"hs.coupon_value",
        $"hs.point_redemption",
        $"hs.gift_voucher",
        $"hs.customer_invoice_incoming_fund_currency",
        $"hs.mdr_installment",
        $"hs.vat_rate",
        $"hs.vat_out_base_contract_currency",
        $"hs.vat_out_contract_currency",
        $"hs.coupon_code",
        $"hs.coupon_description",
        $"hs.gift_voucher_code",
        $"hs.plan_visit_date",
        $"hs.payment_type",
        $"hs.payment_bank_name",
        $"hs.deposit_deduction_type",
        $"hs.primary_product_in_cross_selling",
        $"hs.secondary_product_in_cross_selling",
        $"hs.non_refundable_date",
        $"hs.nett_commission_contract_currency",
        $"hs.premium_to_collecting_incoming_fund_currency",
        $"hs.invoice_due_date",
        $"hs.is_easy_reservation",
        $"hs.booking_expiry_date",
        $"hs.discount_tax_expense",
        $"hs.pot_booking_id",
        coalesce($"mdr_charges_prorate",lit(0)).as("mdr_charges")
      )
  }

  def joinWithColumn(): DataFrame =
    joinDataFrames
}