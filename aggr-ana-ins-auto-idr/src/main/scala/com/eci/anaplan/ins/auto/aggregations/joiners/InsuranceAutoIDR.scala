package com.eci.anaplan.ins.auto.aggregations.joiners

import com.eci.anaplan.ins.auto.aggregations.constructors._
import org.apache.spark.sql.functions.{coalesce, lit, when}
import org.apache.spark.sql.{DataFrame, SparkSession}
import javax.inject.{Inject, Singleton}

@Singleton
class InsuranceAutoIDR @Inject()(spark: SparkSession,
                                 INSNonAutoDf: INSAutoDf,
                                 ExchangeRateDf: ExchangeRateDf,
                                 PurchaseDeliveryDf: PurchaseDeliveryDf,
                                 MDRChargesDf: MDRChargesDf) {

  private def joinDataFrames: DataFrame = {

    import spark.implicits._

    INSNonAutoDf.get.as("ins_auto")
      .join(ExchangeRateDf.get.as("invoice_rate"),
        $"ins_auto.invoice_currency" === $"invoice_rate.from_currency"
          && $"ins_auto.recognition_date" === $"invoice_rate.conversion_date"
        ,"left")
      .join(ExchangeRateDf.get.as("provider_rate"),
        $"ins_auto.provider_currency" === $"provider_rate.from_currency"
          && $"ins_auto.recognition_date" === $"provider_rate.conversion_date"
        ,"left")
      .join(PurchaseDeliveryDf.get.as("pd"),
        $"ins_auto.policy_id" === $"pd.policy_id"
        ,"left")
      .join(MDRChargesDf.get.as("mdr"),
        $"ins_auto.booking_id" === $"mdr.booking_id"
        ,"left")

      .withColumn("mdr_charges_prorate",
        $"mdr.mdr_amount" / $"ins_auto.count_bid"
      )

      .select(
        $"ins_auto.recognition_date",
        $"ins_auto.booking_issued_date",
        $"ins_auto.booking_id",
        $"ins_auto.product_type",
        $"ins_auto.product_name",
        $"ins_auto.insurance_plan",
        $"ins_auto.collecting_payment_entity",
        $"ins_auto.payment_scope",
        $"ins_auto.invoice_currency",
        $"ins_auto.total_actual_fare_paid_by_customer",
        $"ins_auto.discount_or_premium",
        $"ins_auto.discount_wht_expense",
        $"ins_auto.unique_code",
        $"ins_auto.coupon_value",
        $"ins_auto.coupon_wht_expense",
        $"ins_auto.point_redemption",
        $"ins_auto.installment_request",
        $"ins_auto.total_wht_expense",
        $"ins_auto.recognized_expense",
        $"ins_auto.provider_currency",
        $"ins_auto.inventory_owner_entity",
        $"ins_auto.total_fare_from_inventory_owner",
        $"ins_auto.total_fare_from_provider_in_payment_currency",
        $"ins_auto.fulfillment_id",
        $"ins_auto.insurer_name",
        $"ins_auto.business_model",
        $"ins_auto.policy_id",
        $"ins_auto.insurance_expected_recognition_date",
        $"ins_auto.total_fare_from_provider",
        $"ins_auto.total_fare_paid_to_provider",
        $"ins_auto.total_base_fare_from_commission",
        $"ins_auto.insurance_commission",
        $"ins_auto.total_other_income",
        $"ins_auto.collecting_payment_entity_insurance_commission_70_percentage",
        $"ins_auto.collecting_payment_entity_total_other_income_70_percentage",
        $"ins_auto.inventory_owner_entity_insurance_commission_30_percentage",
        $"ins_auto.inventory_owner_entity_total_other_income_30_percentage",
        $"ins_auto.is_interco",
        $"ins_auto.locale",
        $"pd.num_of_coverage".as("num_of_coverage"),
        $"ins_auto.product_type".as("product_category"),
        coalesce(when($"ins_auto.invoice_currency" === "IDR",$"ins_auto.total_actual_fare_paid_by_customer")
          .otherwise($"ins_auto.total_actual_fare_paid_by_customer" * $"invoice_rate.conversion_rate"),lit(0))
          .as("total_actual_fare_paid_by_customer_idr"),
        coalesce(when($"ins_auto.invoice_currency" === "IDR",$"ins_auto.discount_or_premium")
          .otherwise($"ins_auto.discount_or_premium" * $"invoice_rate.conversion_rate"),lit(0))
          .as("discount_or_premium_idr"),
        coalesce(when($"ins_auto.invoice_currency" === "IDR",$"ins_auto.discount_wht_expense")
          .otherwise($"ins_auto.discount_wht_expense" * $"invoice_rate.conversion_rate"),lit(0))
          .as("discount_wht_expense_idr"),
        coalesce(when($"ins_auto.invoice_currency" === "IDR",$"ins_auto.unique_code")
          .otherwise($"ins_auto.unique_code" * $"invoice_rate.conversion_rate"),lit(0))
          .as("unique_code_idr"),
        coalesce(when($"ins_auto.invoice_currency" === "IDR",$"ins_auto.coupon_value")
          .otherwise($"ins_auto.coupon_value" * $"invoice_rate.conversion_rate"),lit(0))
          .as("coupon_value_idr"),
        coalesce(when($"ins_auto.invoice_currency" === "IDR",$"ins_auto.coupon_wht_expense")
          .otherwise($"ins_auto.coupon_wht_expense" * $"invoice_rate.conversion_rate"),lit(0))
          .as("coupon_wht_expense_idr"),
        coalesce(when($"ins_auto.invoice_currency" === "IDR",$"ins_auto.point_redemption")
          .otherwise($"ins_auto.point_redemption" * $"invoice_rate.conversion_rate"),lit(0))
          .as("point_redemption_idr"),
        coalesce(when($"ins_auto.invoice_currency" === "IDR",$"ins_auto.installment_request")
          .otherwise($"ins_auto.installment_request" * $"invoice_rate.conversion_rate"),lit(0))
          .as("installment_request_idr"),
        coalesce(when($"ins_auto.invoice_currency" === "IDR",$"ins_auto.total_wht_expense")
          .otherwise($"ins_auto.total_wht_expense" * $"invoice_rate.conversion_rate"),lit(0))
          .as("total_wht_expense_idr"),
        coalesce(when($"ins_auto.invoice_currency" === "IDR",$"ins_auto.recognized_expense")
          .otherwise($"ins_auto.recognized_expense" * $"invoice_rate.conversion_rate"),lit(0))
          .as("recognized_expense_idr"),
        coalesce(when($"ins_auto.provider_currency" === "IDR",$"ins_auto.total_fare_from_inventory_owner")
          .otherwise($"ins_auto.total_fare_from_inventory_owner" * $"provider_rate.conversion_rate"),lit(0))
          .as("total_fare_from_inventory_owner_idr"),
        coalesce(when($"ins_auto.provider_currency" === "IDR",$"ins_auto.total_fare_from_provider_in_payment_currency")
          .otherwise($"ins_auto.total_fare_from_provider_in_payment_currency" * $"provider_rate.conversion_rate"),lit(0))
          .as("total_fare_from_provider_in_payment_currency_idr"),
        coalesce(when($"ins_auto.provider_currency" === "IDR",$"ins_auto.total_fare_from_provider")
          .otherwise($"ins_auto.total_fare_from_provider" * $"provider_rate.conversion_rate"),lit(0))
          .as("total_fare_from_provider_idr"),
        coalesce(when($"ins_auto.provider_currency" === "IDR",$"ins_auto.total_fare_paid_to_provider")
          .otherwise($"ins_auto.total_fare_paid_to_provider" * $"provider_rate.conversion_rate"),lit(0))
          .as("total_fare_paid_to_provider_idr"),
        coalesce(when($"ins_auto.provider_currency" === "IDR",$"ins_auto.total_base_fare_from_commission")
          .otherwise($"ins_auto.total_base_fare_from_commission" * $"provider_rate.conversion_rate"),lit(0))
          .as("total_base_fare_from_commission_idr"),
        coalesce(when($"ins_auto.provider_currency" === "IDR",$"ins_auto.insurance_commission")
          .otherwise($"ins_auto.insurance_commission" * $"provider_rate.conversion_rate"),lit(0))
          .as("insurance_commission_idr"),
        coalesce(when($"ins_auto.provider_currency" === "IDR",$"ins_auto.total_other_income")
          .otherwise($"ins_auto.total_other_income" * $"provider_rate.conversion_rate"),lit(0))
          .as("total_other_income_idr"),
        coalesce(
          when($"ins_auto.provider_currency" === "IDR",$"ins_auto.collecting_payment_entity_insurance_commission_70_percentage")
            .otherwise($"ins_auto.collecting_payment_entity_insurance_commission_70_percentage" * $"provider_rate.conversion_rate"),
          lit(0))
          .as("collecting_payment_entity_insurance_commission_70_idr"),
        coalesce(
          when($"ins_auto.provider_currency" === "IDR",$"ins_auto.collecting_payment_entity_total_other_income_70_percentage")
            .otherwise($"ins_auto.collecting_payment_entity_total_other_income_70_percentage" * $"provider_rate.conversion_rate"),
          lit(0))
          .as("collecting_payment_entity_total_other_income_70_idr"),
        coalesce(
          when($"ins_auto.provider_currency" === "IDR",$"ins_auto.inventory_owner_entity_insurance_commission_30_percentage")
            .otherwise($"ins_auto.inventory_owner_entity_insurance_commission_30_percentage" * $"provider_rate.conversion_rate"),
          lit(0))
          .as("inventory_owner_entity_insurance_commission_30_idr"),
        coalesce(
          when($"ins_auto.provider_currency" === "IDR",$"ins_auto.inventory_owner_entity_total_other_income_30_percentage")
            .otherwise($"ins_auto.inventory_owner_entity_total_other_income_30_percentage" * $"provider_rate.conversion_rate"),
          lit(0))
          .as("inventory_owner_entity_total_other_income_30_idr"),
        coalesce($"mdr_charges_prorate",lit(0)).as("mdr_charges_prorate"),
        coalesce(when($"ins_auto.invoice_currency" === "IDR",$"mdr_charges_prorate")
          .otherwise($"mdr_charges_prorate" * $"invoice_rate.conversion_rate"),lit(0))
          .as("mdr_charges_idr")
      )
  }

  def joinWithColumn(): DataFrame =
    joinDataFrames
}
