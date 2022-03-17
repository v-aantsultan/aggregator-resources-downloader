package com.eci.anaplan.wm.aggregations.joiners

import com.eci.anaplan.wm.aggregations.constructors._
import org.apache.spark.sql.functions.{coalesce, lit, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class WealthManagementIDR @Inject()(spark: SparkSession,
                                    WealthManagementDf: WealthManagementDf,
                                    ExchangeRateDf: ExchangeRateDf,
                                    MDRChargesDf: MDRChargesDf) {

  private def joinDataFrames: DataFrame = {

    import spark.implicits._

    WealthManagementDf.get.as("wm")
      .join(ExchangeRateDf.get.as("invoice_rate"),
        $"wm.invoice_currency" === $"invoice_rate.from_currency"
          && $"wm.booking_issued_date" === $"invoice_rate.conversion_date"
        ,"left")
      .join(MDRChargesDf.get.as("mdr"),
        $"wm.booking_id" === $"mdr.booking_id"
        ,"left")

      .withColumn("mdr_amount_prorate",
        (($"wm.sum_total_actual_fare_bid" / $"mdr.expected_amount") * $"mdr.mdr_amount") / $"wm.count_bid"
      )

      .select(
        $"wm.booking_issued_date",
        $"wm.booking_id",
        $"wm.investment_id",
        $"wm.product_type",
        $"wm.product_name",
        $"wm.collecting_payment_entity",
        $"wm.payment_scope",
        $"wm.invoice_currency",
        $"wm.total_actual_fare_paid_by_customer",
        $"wm.unique_code",
        $"wm.coupon_value",
        $"wm.coupon_wht_expense",
        $"wm.point_redemption",
        $"wm.gift_voucher",
        $"wm.total_wht_expense",
        $"wm.inventory_owner_entity",
        $"wm.total_fare_from_inventory_owner",
        $"wm.total_fare_from_provider_in_payment_currency",
        $"wm.fulfillment_id",
        $"wm.business_model",
        $"wm.transaction_type",
        $"wm.provider_transaction_id",
        $"wm.topup_issuance_date",
        $"wm.provider_currency",
        $"wm.topup_amount",
        $"wm.total_fare_paid_to_provider",
        $"wm.total_base_fare_for_commission",
        $"wm.commission_revenue",
        $"wm.collecting_payment_entity_commission_70_percentage",
        $"wm.collecting_payment_entity_commission_30_percentage",
        $"wm.is_interco",
        $"wm.locale",
        coalesce($"mdr_amount_prorate",lit(0)).as("mdr_amount_prorate"),
        coalesce(when($"wm.invoice_currency" === "IDR",$"mdr_amount_prorate")
          .otherwise($"mdr_amount_prorate" * $"invoice_rate.conversion_rate"),lit(0))
          .as("mdr_amount_prorate_idr")
      )
  }

  def joinWithColumn(): DataFrame =
    joinDataFrames
}