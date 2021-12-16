package com.eci.anaplan.ins.nonauto.aggregations.joiners

import com.eci.anaplan.ins.nonauto.aggregations.constructors._
import org.apache.spark.sql.functions.{coalesce, lit, when}
import org.apache.spark.sql.{DataFrame, SparkSession}
import javax.inject.{Inject, Singleton}

@Singleton
class InsuranceNonAutoIDR @Inject()(spark: SparkSession,
                                    INSNonAutoDf: INSNonAutoDf,
                                    ExchangeRateDf: INSNonAutoRateDf) {

  private def joinDataFrames: DataFrame = {

    import spark.implicits._

    INSNonAutoDf.get.as("ins_nonauto")
      .join(ExchangeRateDf.get.as("invoice_rate"),
        $"ins_nonauto.invoice_currency" === $"invoice_rate.from_currency"
          && $"ins_nonauto.booking_issued_date" === $"invoice_rate.conversion_date"
        ,"left")
      .join(ExchangeRateDf.get.as("provider_rate"),
        $"ins_nonauto.provider_currency" === $"provider_rate.from_currency"
          && $"ins_nonauto.booking_issued_date" === $"provider_rate.conversion_date"
        ,"left")

      .select(
        $"ins_nonauto.*",
        lit(0).as("num_of_adults"),
        lit(0).as("num_of_children"),
        lit(0).as("num_of_infants"),
        lit(0).as("num_of_coverage"),
        when($"ins_nonauto.booking_type".isin("CROSSSELL_ADDONS","ADDONS","CROSSSELL_BUNDLE"),lit("IA"))
          .otherwise(lit("IS"))
          .as("product_category"),
        coalesce(when($"ins_nonauto.invoice_currency" === "IDR",$"ins_nonauto.total_actual_fare_paid_by_customer")
          .otherwise($"ins_nonauto.total_actual_fare_paid_by_customer" * $"invoice_rate.conversion_rate"),lit(0))
          .as("total_actual_fare_paid_by_customer_idr"),
        coalesce(when($"ins_nonauto.invoice_currency" === "IDR",$"ins_nonauto.discount_or_premium")
          .otherwise($"ins_nonauto.discount_or_premium" * $"invoice_rate.conversion_rate"),lit(0))
          .as("discount_or_premium_idr"),
        coalesce(when($"ins_nonauto.invoice_currency" === "IDR",$"ins_nonauto.discount_wht_expense")
          .otherwise($"ins_nonauto.discount_wht_expense" * $"invoice_rate.conversion_rate"),lit(0))
          .as("discount_wht_expense_idr"),
        coalesce(when($"ins_nonauto.invoice_currency" === "IDR",$"ins_nonauto.unique_code")
          .otherwise($"ins_nonauto.unique_code" * $"invoice_rate.conversion_rate"),lit(0))
          .as("unique_code_idr"),
        coalesce(when($"ins_nonauto.invoice_currency" === "IDR",$"ins_nonauto.coupon_value")
          .otherwise($"ins_nonauto.coupon_value" * $"invoice_rate.conversion_rate"),lit(0))
          .as("coupon_value_idr"),
        coalesce(when($"ins_nonauto.invoice_currency" === "IDR",$"ins_nonauto.coupon_wht_expense")
          .otherwise($"ins_nonauto.coupon_wht_expense" * $"invoice_rate.conversion_rate"),lit(0))
          .as("coupon_wht_expense_idr"),
        coalesce(when($"ins_nonauto.invoice_currency" === "IDR",$"ins_nonauto.point_redemption")
          .otherwise($"ins_nonauto.point_redemption" * $"invoice_rate.conversion_rate"),lit(0))
          .as("point_redemption_idr"),
        coalesce(when($"ins_nonauto.invoice_currency" === "IDR",$"ins_nonauto.installment_request")
          .otherwise($"ins_nonauto.installment_request" * $"invoice_rate.conversion_rate"),lit(0))
          .as("installment_request_idr"),
        coalesce(when($"ins_nonauto.invoice_currency" === "IDR",$"ins_nonauto.total_wht_expense")
          .otherwise($"ins_nonauto.total_wht_expense" * $"invoice_rate.conversion_rate"),lit(0))
          .as("total_wht_expense_idr"),
        coalesce(when($"ins_nonauto.provider_currency" === "IDR",$"ins_nonauto.total_fare_from_inventory_owner")
          .otherwise($"ins_nonauto.total_fare_from_inventory_owner" * $"provider_rate.conversion_rate"),lit(0))
          .as("total_fare_from_inventory_owner_idr"),
        coalesce(when($"ins_nonauto.provider_currency" === "IDR",$"ins_nonauto.total_fare_from_provider_in_payment_currency")
          .otherwise($"ins_nonauto.total_fare_from_provider_in_payment_currency" * $"provider_rate.conversion_rate"),lit(0))
          .as("total_fare_from_provider_in_payment_currency_idr"),
        coalesce(when($"ins_nonauto.provider_currency" === "IDR",$"ins_nonauto.total_fare_from_provider")
          .otherwise($"ins_nonauto.total_fare_from_provider" * $"provider_rate.conversion_rate"),lit(0))
          .as("total_fare_from_provider_idr"),
        coalesce(when($"ins_nonauto.provider_currency" === "IDR",$"ins_nonauto.total_fare_paid_to_provider")
          .otherwise($"ins_nonauto.total_fare_paid_to_provider" * $"provider_rate.conversion_rate"),lit(0))
          .as("total_fare_paid_to_provider_idr"),
        coalesce(when($"ins_nonauto.provider_currency" === "IDR",$"ins_nonauto.total_base_fare_for_commission")
          .otherwise($"ins_nonauto.total_base_fare_for_commission" * $"provider_rate.conversion_rate"),lit(0))
          .as("total_base_fare_for_commission_idr"),
        coalesce(when($"ins_nonauto.provider_currency" === "IDR",$"ins_nonauto.insurance_commission")
          .otherwise($"ins_nonauto.insurance_commission" * $"provider_rate.conversion_rate"),lit(0))
          .as("insurance_commission_idr"),
        coalesce(when($"ins_nonauto.provider_currency" === "IDR",$"ins_nonauto.total_other_income")
          .otherwise($"ins_nonauto.total_other_income" * $"provider_rate.conversion_rate"),lit(0))
          .as("total_other_income_idr"),
        coalesce(
          when($"ins_nonauto.provider_currency" === "IDR",$"ins_nonauto.collecting_payment_entity_insurance_commission_70_percentage")
            .otherwise($"ins_nonauto.collecting_payment_entity_insurance_commission_70_percentage" * $"provider_rate.conversion_rate"),
          lit(0))
          .as("collecting_payment_entity_insurance_commission_70_percentage_idr"),
        coalesce(
          when($"ins_nonauto.provider_currency" === "IDR",$"ins_nonauto.collecting_payment_entity_total_other_income_70_percentage")
            .otherwise($"ins_nonauto.collecting_payment_entity_total_other_income_70_percentage" * $"provider_rate.conversion_rate"),
          lit(0))
          .as("collecting_payment_entity_total_other_income_70_percentage_idr"),
        coalesce(
          when($"ins_nonauto.provider_currency" === "IDR",$"ins_nonauto.inventory_owner_entity_insurance_commission_30_percentage")
            .otherwise($"ins_nonauto.inventory_owner_entity_insurance_commission_30_percentage" * $"provider_rate.conversion_rate"),
          lit(0))
          .as("inventory_owner_entity_insurance_commission_30_percentage_idr"),
        coalesce(
          when($"ins_nonauto.provider_currency" === "IDR",$"ins_nonauto.inventory_owner_entity_total_other_income_30_percentage")
            .otherwise($"ins_nonauto.inventory_owner_entity_total_other_income_30_percentage" * $"provider_rate.conversion_rate"),
          lit(0))
          .as("inventory_owner_entity_total_other_income_30_percentage_idr")
      )
  }

  def joinWithColumn(): DataFrame =
    joinDataFrames
}
