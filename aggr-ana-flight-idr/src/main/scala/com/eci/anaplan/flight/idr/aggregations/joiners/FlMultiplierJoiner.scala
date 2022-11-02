package com.eci.anaplan.flight.idr.aggregations.joiners

import com.eci.anaplan.flight.idr.aggregations.constructors._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class FlMultiplierJoiner @Inject()(spark: SparkSession,
                                   FlightDf: FlightDf,
                                   FlattenerDimCountryDf: FlattenerDimCountryDf,
                                   FlattenerFulfillmentDf: FlattenerFulfillmentDf,
                                   ExchangeRateDf: ExchangeRateDf,
                                   MDRChargesDf: MDRChargesDf) {

  private def joinDataFrames: DataFrame = {

    import spark.implicits._

    val BeforeJoined = FlightDf.get
      .withColumn("booking_issue_date_formated",
        to_date($"booking_issue_date", "dd/MM/yyyy")
      )
      .withColumn("destination_airport_country_name",
        regexp_replace(
          regexp_replace(
            regexp_replace($"airport_countries", "-", ""),
            ",", ""),
          " ", "")
      )
      .withColumn("split_locale",
        split($"locale", "_")(1)
      )
      .withColumn("count_bid",
        count($"booking_id").over(Window.partitionBy($"booking_id"))
      )
      .withColumn("sum_invoice_amount",
        sum($"invoice_amount").over(Window.partitionBy($"booking_id"))
      )

    val JoinedFlattenerSheet = BeforeJoined.as("fl")
      .join(
        FlattenerDimCountryDf.get.as("flat_dim_country"),
        $"fl.split_locale" === $"flat_dim_country.country_id",
        "left")
      .join(
        FlattenerFulfillmentDf.get.as("flat_fulfillment"),
        lower($"fl.fulfillment_id") === lower($"flat_fulfillment.fulfillment_id"),
        "left")

      .withColumn("replace_destination_airport_country_name",
        regexp_replace($"fl.destination_airport_country_name", $"flat_dim_country.country_name", lit(""))
      )
      .withColumn("domestic_international",
        when(length($"replace_destination_airport_country_name") < 1, concat(lit("Dom-"), $"fl.split_locale"))
          .otherwise(concat(lit("Int-"), $"fl.split_locale"))
      )
      .withColumn("wholesaler",
        when($"flat_fulfillment.wholesaler".isNull || $"flat_fulfillment.wholesaler" === "", "N/A")
        .otherwise($"flat_fulfillment.wholesaler")
      )
      .drop("replace_destination_airport_country_name")
      .drop($"flat_fulfillment.fulfillment_id")

    val MDR = MDRChargesDf.get

    val JoinedMDR = JoinedFlattenerSheet
      .join(MDR,
        JoinedFlattenerSheet("booking_id") === MDR("booking_id")
        , "left")
      .drop(MDR("booking_id"))

      .withColumn("mdr_charges",
        (($"sum_invoice_amount" / $"expected_amount") * $"mdr_amount") / $"count_bid"
      )
      .drop("count_bid")
      .drop("sum_customer_invoice")
      .drop("expected_amount")
      .drop("mdr_amount")

    val ExchangeRateIDR = ExchangeRateDf.get
    val ContractRates = coalesce($"contract_rates.conversion_rate", lit(1))
    val CollectionRates = coalesce($"collecting_rates.conversion_rate", lit(1))
    val SettlementRates = coalesce($"settlement_rates.conversion_rate", lit(1))

    val JoinedExchangeRate = JoinedMDR.as("fl")
      .join(ExchangeRateIDR.as("contract_rates"),
        $"fl.contract_currency" === $"contract_rates.from_currency" &&
          $"fl.booking_issue_date_formated" === $"contract_rates.conversion_date",
        "left")
      .join(ExchangeRateIDR.as("collecting_rates"),
        $"fl.collecting_currency" === $"collecting_rates.from_currency" &&
          $"fl.booking_issue_date_formated" === $"collecting_rates.conversion_date",
        "left")
      .join(ExchangeRateIDR.as("settlement_rates"),
        $"fl.settlement_currency" === $"settlement_rates.from_currency" &&
          $"fl.booking_issue_date_formated" === $"settlement_rates.conversion_date",
        "left")

      .select(
        $"fl.*",
        ($"fl.total_fare__contract_currency_" * ContractRates).as("total_fare__contract_currency__idr"),
        ($"fl.airline_reschedule_fee__contract_currency_" * ContractRates).as("airline_reschedule_fee__contract_currency__idr"),
        ($"fl.total_agent_fare__contract_currency_" * ContractRates).as("total_agent_fare__contract_currency__idr"),
        ($"fl.traveloka_reschedule_fee__contract_currency_" * ContractRates).as("traveloka_reschedule_fee__contract_currency__idr"),
        ($"fl.new_gross_commission" * ContractRates).as("new_gross_commission_idr"),
        ($"fl.commission_tax" * ContractRates).as("commission_tax_idr"),
        ($"fl.net_commission" * ContractRates).as("net_commission_idr"),
        ($"fl.nta" * ContractRates).as("nta_idr"),
        ($"fl.bta" * ContractRates).as("bta_idr"),
        ($"fl.unique_code__contract_currency_" * ContractRates).as("unique_code__contract_currency__idr"),
        ($"fl.base_fare_adult__per_pax_" * ContractRates).as("base_fare_adult__per_pax__idr"),
        ($"fl.base_fare_child__per_pax_" * ContractRates).as("base_fare_child__per_pax__idr"),
        ($"fl.base_fare_infant__per_pax_" * ContractRates).as("base_fare_infant__per_pax__idr"),
        ($"fl.issuance_fee" * ContractRates).as("issuance_fee_idr"),
        ($"fl.vat_fee" * ContractRates).as("vat_fee_idr"),
        ($"fl.billing_amount" * ContractRates).as("billing_amount_idr"),
        ($"fl.total_base_fare__exclude_infant_" * ContractRates).as("total_base_fare__exclude_infant__idr"),
        ($"fl.basic_incentive" * ContractRates).as("basic_incentive_idr"),
        ($"fl.vat_out" * ContractRates).as("vat_out_idr"),
        ($"fl.credit_shell_amount" * ContractRates).as("credit_shell_amount_idr"),
        ($"fl.inventory_amount" * ContractRates).as("inventory_amount_idr"),
        ($"fl.infant_fare_before_vat" * ContractRates).as("infant_fare_before_vat_idr"),
        ($"fl.commission_to_collecting_entity" * CollectionRates).as("commission_to_collecting_entity_idr"),
        ($"fl.total_agent_fare" * CollectionRates).as("total_agent_fare_idr"),
        ($"fl.airline_reschedule_fee__collecting_currency_" * CollectionRates).as("airline_reschedule_fee__collecting_currency__idr"),
        ($"fl.invoice_amount" * CollectionRates).as("invoice_amount_idr"),
        ($"fl.total_fare__collecting_currency_" * CollectionRates).as("total_fare__collecting_currency__idr"),
        ($"fl.discount_premium" * CollectionRates).as("discount_premium_idr"),
        ($"fl.non_connecting_discount_premium" * CollectionRates).as("non_connecting_discount_premium_idr"),
        ($"fl.reschedule_mark_up" * CollectionRates).as("reschedule_mark_up_idr"),
        ($"fl.package_discount_premium" * CollectionRates).as("package_discount_premium_idr"),
        ($"fl.unique_code" * CollectionRates).as("unique_code_idr"),
        ($"fl.coupon_value" * CollectionRates).as("coupon_value_idr"),
        ($"fl.gift_voucher_value" * CollectionRates).as("gift_voucher_value_idr"),
        ($"fl.traveloka_reschedule_fee" * CollectionRates).as("traveloka_reschedule_fee_idr"),
        ($"fl.transaction_fee" * CollectionRates).as("transaction_fee_idr"),
        ($"fl.rebook_cost" * CollectionRates).as("rebook_cost_idr"),
        ($"fl.discount_wht" * CollectionRates).as("discount_wht_idr"),
        ($"fl.coupon_wht" * CollectionRates).as("coupon_wht_idr"),
        ($"fl.payment_fee" * CollectionRates).as("payment_fee_idr"),
        ($"fl.mdr_installment" * CollectionRates).as("mdr_installment_idr"),
        ($"fl.installment_request" * CollectionRates).as("installment_request_idr"),
        ($"fl.commission_to_affiliate" * CollectionRates).as("commission_to_affiliate_idr"),
        ($"fl.point_redemption" * CollectionRates).as("point_redemption_idr"),
        ($"fl.no_hold_booking_cost" * CollectionRates).as("no_hold_booking_cost_idr"),
        ($"fl.total_fare__settlement_currency_" * SettlementRates).as("total_fare__settlement_currency__idr"),
        ($"fl.airline_reschedule_fee__settlement_currency_" * SettlementRates).as("airline_reschedule_fee__settlement_currency__idr"),
        ($"fl.traveloka_reschedule_fee__settlement_currency_" * SettlementRates).as("traveloka_reschedule_fee__settlement_currency__idr"),
        ($"fl.unique_code__settlement_currency_" * SettlementRates).as("unique_code__settlement_currency__idr"),
        ($"fl.base_fare_adult_per_pax__settlement_currency_" * SettlementRates).as("base_fare_adult_per_pax__settlement_currency__idr"),
        ($"fl.base_fare_child_per_pax__settlement_currency_" * SettlementRates).as("base_fare_child_per_pax__settlement_currency__idr"),
        ($"fl.base_fare_infant_per_pax__settlement_currency_" * SettlementRates).as("base_fare_infant_per_pax__settlement_currency__idr"),
        ($"fl.total_base_fare_exclude_infant__settlement_currency_" * SettlementRates)
          .as("total_base_fare_exclude_infant__settlement_currency__idr"),
        ($"fl.new_gross_commission__settlement_currency_" * SettlementRates).as("new_gross_commission__settlement_currency__idr"),
        ($"fl.commission_tax__settlement_currency_" * SettlementRates).as("commission_tax__settlement_currency__idr"),
        ($"fl.net_commission__settlement_currency_" * SettlementRates).as("net_commission__settlement_currency__idr"),
        ($"fl.basic_incentive__settlement_currency_" * SettlementRates).as("basic_incentive__settlement_currency__idr"),
        ($"fl.nta__settlement_currency_" * SettlementRates).as("nta__settlement_currency__idr"),
        ($"fl.bta__settlement_currency_" * SettlementRates).as("bta__settlement_currency__idr"),
        ($"fl.billing_amount__settlement_currency_" * SettlementRates).as("billing_amount__settlement_currency__idr"),
        ($"fl.issuance_fee__settlement_currency_" * SettlementRates).as("issuance_fee__settlement_currency__idr"),
        ($"fl.total_agent_fare__settlement_currency_" * SettlementRates).as("total_agent_fare__settlement_currency__idr"),
        ($"fl.credit_shell_amount__settlement_currency_" * SettlementRates).as("credit_shell_amount__settlement_currency__idr"),
        ($"fl.mdr_charges" * CollectionRates).as("mdr_charges_idr")
      )

    JoinedExchangeRate

  }

  def get(): DataFrame =
    joinDataFrames
}