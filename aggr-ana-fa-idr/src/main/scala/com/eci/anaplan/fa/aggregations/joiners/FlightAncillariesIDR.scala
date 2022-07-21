package com.eci.anaplan.fa.aggregations.joiners

import com.eci.anaplan.fa.aggregations.constructors.{ExchangeRateDf, FlightAncillariesDf, MDRChargesDf, MappingFulfillmentIDDf}
import org.apache.spark.sql.functions.{coalesce, lit, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class FlightAncillariesIDR @Inject()(spark: SparkSession,
                               FlightAncillariesDf: FlightAncillariesDf,
                               ExchangeRateDf: ExchangeRateDf,
                               MDRChargesDf: MDRChargesDf,
                               mappingFulfillmentIDDf: MappingFulfillmentIDDf) {

  private def joinDataFrames: DataFrame = {

    import spark.implicits._

    FlightAncillariesDf.get.as("fa")
      .join(ExchangeRateDf.get.as("contract_rate"),
        $"fa.contract_currency" === $"contract_rate.from_currency"
          && $"fa.booking_issue_date" === $"contract_rate.conversion_date"
        ,"left")
      .join(ExchangeRateDf.get.as("collecting_rate"),
        $"fa.collecting_currency" === $"collecting_rate.from_currency"
          && $"fa.booking_issue_date" === $"collecting_rate.conversion_date"
        ,"left")
      .join(ExchangeRateDf.get.as("settlement_rate"),
        $"fa.settlement_currency" === $"settlement_rate.from_currency"
          && $"fa.booking_issue_date" === $"settlement_rate.conversion_date"
        ,"left")
      .join(MDRChargesDf.get.as("mdr"),
        $"fa.booking_id" === $"mdr.booking_id"
        ,"left")
      .join(mappingFulfillmentIDDf.get.as("ful_id"),
        $"fa.fulfillment_id" === $"ful_id.fulfillment_id"
        ,"left")

      .withColumn("mdr_charges",
        (($"fa.sum_customer_invoice_bid" / $"mdr.expected_amount") * $"mdr.mdr_amount") / $"fa.count_bid"
      )

      .select(
        $"fa.booking_issue_date",
        $"fa.locale",
        $"fa.product_type",
        $"fa.booking_id",
        $"fa.fulfillment_id",
        $"fa.supplier_id",
        $"fa.supplier_name",
        $"fa.business_model",
        $"fa.brand_codes",
        $"fa.provider_booking_id",
        $"fa.product_category",
        $"fa.product_id",
        $"fa.product_name",
        $"fa.contract_entity",
        $"fa.plan_visit_date",
        $"fa.non_refundable_date",
        $"fa.route",
        $"fa.product_countries",
        $"fa.original_ticket_route",
        $"fa.collecting_entity",
        $"fa.payment_channel",
        $"fa.issuer_bank",
        $"fa.installment_code",
        $"fa.quantity",
        $"fa.contract_currency",
        coalesce(when($"fa.contract_currency" === "IDR",$"fa.total_fare__contract_currency_")
          .otherwise($"fa.total_fare__contract_currency_" * $"contract_rate.conversion_rate"),lit(0))
          .as("total_fare__contract_currency__idr"),
        coalesce(when($"fa.contract_currency" === "IDR",$"fa.nta")
          .otherwise($"fa.nta" * $"contract_rate.conversion_rate"),lit(0))
          .as("nta_idr"),
        coalesce(when($"fa.contract_currency" === "IDR",$"fa.total_fare___nta")
          .otherwise($"fa.total_fare___nta" * $"contract_rate.conversion_rate"),lit(0))
          .as("total_fare___nta_idr"),
        coalesce(when($"fa.contract_currency" === "IDR",$"fa.commission")
          .otherwise($"fa.commission" * $"contract_rate.conversion_rate"),lit(0))
          .as("commission_idr"),
        coalesce(when($"fa.contract_currency" === "IDR",$"fa.issuance_fee")
          .otherwise($"fa.issuance_fee" * $"contract_rate.conversion_rate"),lit(0))
          .as("issuance_fee_idr"),
        coalesce(when($"fa.contract_currency" === "IDR",$"fa.bta")
          .otherwise($"fa.bta" * $"contract_rate.conversion_rate"),lit(0))
          .as("bta_idr"),
        $"fa.collecting_currency",
        coalesce(when($"fa.collecting_currency" === "IDR",$"fa.total_fare__collecting_currency_")
          .otherwise($"fa.total_fare__collecting_currency_" * $"collecting_rate.conversion_rate"),lit(0))
          .as("total_fare__collecting_currency__idr"),
        coalesce(when($"fa.collecting_currency" === "IDR",$"fa.discount_premium")
          .otherwise($"fa.discount_premium" * $"collecting_rate.conversion_rate"),lit(0))
          .as("discount_premium_idr"),
        coalesce(when($"fa.collecting_currency" === "IDR",$"fa.selling_price")
          .otherwise($"fa.selling_price" * $"collecting_rate.conversion_rate"),lit(0))
          .as("selling_price_idr"),
        coalesce(when($"fa.collecting_currency" === "IDR",$"fa.installment_fee")
          .otherwise($"fa.installment_fee" * $"collecting_rate.conversion_rate"),lit(0))
          .as("installment_fee_idr"),
        coalesce(when($"fa.collecting_currency" === "IDR",$"fa.unique_code")
          .otherwise($"fa.unique_code" * $"collecting_rate.conversion_rate"),lit(0))
          .as("unique_code_idr"),
        coalesce(when($"fa.collecting_currency" === "IDR",$"fa.coupon_value")
          .otherwise($"fa.coupon_value" * $"collecting_rate.conversion_rate"),lit(0))
          .as("coupon_value_idr"),
        coalesce(when($"fa.collecting_currency" === "IDR",$"fa.point_redemption")
          .otherwise($"fa.point_redemption" * $"collecting_rate.conversion_rate"),lit(0))
          .as("point_redemption_idr"),
        coalesce(when($"fa.collecting_currency" === "IDR",$"fa.customer_invoice")
          .otherwise($"fa.customer_invoice" * $"collecting_rate.conversion_rate"),lit(0))
          .as("customer_invoice_idr"),
        $"fa.customer_invoice".as("invoice_amount"),
        coalesce($"mdr_charges",lit(0)).as("mdr_charges"),
        coalesce(when($"fa.collecting_currency" === "IDR",$"mdr_charges")
          .otherwise($"mdr_charges" * $"collecting_rate.conversion_rate"),lit(0))
          .as("mdr_charges_idr"),
        coalesce(when($"fa.collecting_currency" === "IDR",$"fa.mdr_installment")
          .otherwise($"fa.mdr_installment" * $"collecting_rate.conversion_rate"),lit(0))
          .as("mdr_installment_idr"),
        coalesce(when($"ful_id.wholesaler".isNull || $"ful_id.wholesaler" === "N/A"
          || $"ful_id.wholesaler" === "","N/A")
          .otherwise($"ful_id.wholesaler"))
          .as("wholesaler"),
        $"fa.coupon_code",
        $"fa.coupon_description",
        $"fa.reschedule_id",
        $"fa.settlement_method",
        $"fa.primary_product",
        $"fa.secondary_product",
        $"fa.affiliate_id",
        $"fa.refunded_date",
        $"fa.refund_type",
        coalesce(when($"fa.contract_currency" === "IDR",$"fa.refund_amount_from_provider")
          .otherwise($"fa.refund_amount_from_provider" * $"contract_rate.conversion_rate"),lit(0))
          .as("refund_amount_from_provider_idr"),
        $"fa.corporate_id",
        coalesce(when($"fa.collecting_currency" === "IDR",$"fa.commission_to_affiliate")
          .otherwise($"fa.commission_to_affiliate" * $"collecting_rate.conversion_rate"),lit(0))
          .as("commission_to_affiliate_idr"),
        coalesce(when($"fa.collecting_currency" === "IDR",$"fa.rebook_cost")
          .otherwise($"fa.rebook_cost" * $"collecting_rate.conversion_rate"),lit(0))
          .as("rebook_cost_idr"),
        $"fa.corporate_type",
        $"fa.customer_country",
        coalesce(when($"fa.collecting_currency" === "IDR",$"fa.transaction_fee")
          .otherwise($"fa.transaction_fee" * $"collecting_rate.conversion_rate"),lit(0))
          .as("transaction_fee_idr"),
        coalesce(when($"fa.collecting_currency" === "IDR",$"fa.gift_voucher_value")
          .otherwise($"fa.gift_voucher_value" * $"collecting_rate.conversion_rate"),lit(0))
          .as("gift_voucher_value_idr"),
        $"fa.settlement_currency",
        coalesce(when($"fa.settlement_currency" === "IDR",$"fa.total_fare__settlement_currency_")
          .otherwise($"fa.total_fare__settlement_currency_" * $"settlement_rate.conversion_rate"),lit(0))
          .as("total_fare__settlement_currency__idr"),
        coalesce(when($"fa.settlement_currency" === "IDR",$"fa.nta__settlement_currency_")
          .otherwise($"fa.nta__settlement_currency_" * $"settlement_rate.conversion_rate"),lit(0))
          .as("nta__settlement_currency__idr"),
        coalesce(when($"fa.settlement_currency" === "IDR",$"fa.total_fare___nta__settlement_currency_")
          .otherwise($"fa.total_fare___nta__settlement_currency_" * $"settlement_rate.conversion_rate"),lit(0))
          .as("total_fare___nta__settlement_currency__idr"),
        coalesce(when($"fa.settlement_currency" === "IDR",$"fa.commission__settlement_currency_")
          .otherwise($"fa.commission__settlement_currency_" * $"settlement_rate.conversion_rate"),lit(0))
          .as("commission__settlement_currency__idr"),
        coalesce(when($"fa.settlement_currency" === "IDR",$"fa.issuance_fee__settlement_currency_")
          .otherwise($"fa.issuance_fee__settlement_currency_" * $"settlement_rate.conversion_rate"),lit(0))
          .as("issuance_fee__settlement_currency__idr"),
        coalesce(when($"fa.settlement_currency" === "IDR",$"fa.bta__settlement_currency_")
          .otherwise($"fa.bta__settlement_currency_" * $"settlement_rate.conversion_rate"),lit(0))
          .as("bta__settlement_currency__idr"),
        coalesce(when($"fa.collecting_currency" === "IDR",$"fa.discount_wht")
          .otherwise($"fa.discount_wht" * $"collecting_rate.conversion_rate"),lit(0))
          .as("discount_wht_idr"),
        coalesce(when($"fa.collecting_currency" === "IDR",$"fa.coupon_wht")
          .otherwise($"fa.coupon_wht" * $"collecting_rate.conversion_rate"),lit(0))
          .as("coupon_wht_idr"),
        $"fa.flight_code",
        $"fa.business_partner_id",
        $"fa.selling_entity"
      )
  }

  def joinWithColumn(): DataFrame =
    joinDataFrames
}