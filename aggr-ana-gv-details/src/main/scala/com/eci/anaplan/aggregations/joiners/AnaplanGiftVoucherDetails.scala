package com.eci.anaplan.aggregations.joiners

import com.eci.anaplan.aggregations.constructors._
import com.eci.anaplan.services.GVDetailsStatusManager
import javax.inject.{Inject, Singleton}
import org.apache.spark.sql.functions.{countDistinct, sum, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

@Singleton
class AnaplanGiftVoucherDetails @Inject()(spark: SparkSession, statusManagerService: GVDetailsStatusManager,
                                          GVRedeemIDRDf: GVRedeemIDRDf, GVRevenueIDRDf: GVRevenueIDRDf,
                                          GVSalesB2CIDRDf: GVSalesB2CIDRDf, GVSalesB2BIDRDf: GVSalesB2BIDRDf) {

  private def joinDataFrames: DataFrame = {

    import spark.implicits._

    val GVRedeemIDR = GVRedeemIDRDf.get
      .groupBy($"date", $"product", $"business_partner", $"voucher_redemption_product", $"customer", $"payment_channel_name")
      .agg(
        sum($"gift_voucher_amount").as("gift_voucher_amount"),
        countDistinct($"no_of_transactions").as("no_of_transactions"),
        countDistinct($"no_gift_voucher").as("no_gift_voucher"),
        sum($"revenue_amount").as("revenue_amount"),
        sum($"unique_code").as("unique_code"),
        sum($"coupon_value").as("coupon_value"),
        sum($"discount").as("discount"),
        sum($"premium").as("premium")
      )
      .select(
        $"*"
      )

    val GVRevenueIDR = GVRevenueIDRDf.get
      .groupBy($"date", $"product", $"business_partner", $"voucher_redemption_product", $"customer", $"payment_channel_name")
      .agg(
        sum($"gift_voucher_amount").as("gift_voucher_amount"),
        countDistinct($"no_of_transactions").as("no_of_transactions"),
        countDistinct($"no_gift_voucher").as("no_gift_voucher"),
        sum($"revenue_amount").as("revenue_amount"),
        sum($"unique_code").as("unique_code"),
        sum($"coupon_value").as("coupon_value"),
        sum($"discount").as("discount"),
        sum($"premium").as("premium")
      )
      .select(
        $"*"
      )

    val GVSalesB2CIDR = GVSalesB2CIDRDf.get
      .groupBy($"date", $"product", $"business_partner", $"voucher_redemption_product", $"customer", $"payment_channel_name")
      .agg(
        sum($"gift_voucher_amount").as("gift_voucher_amount"),
        countDistinct($"no_of_transactions").as("no_of_transactions"),
        countDistinct($"no_gift_voucher").as("no_gift_voucher"),
        sum($"revenue_amount").as("revenue_amount"),
        sum($"unique_code").as("unique_code"),
        sum($"coupon_value").as("coupon_value"),
        sum(
          when($"discount_or_premium_in_idr" < 0 ,$"discount_or_premium_in_idr" + $"discount_wht_in_idr")
            .otherwise(0)
        ).as("discount"),
        sum(
          when($"discount_or_premium_in_idr" >= 0 ,$"discount_or_premium_in_idr")
            .otherwise(0)
        ).as("premium")
      )
      .select(
        $"*"
      )

    val GVSalesB2BIDR = GVSalesB2BIDRDf.get
      .groupBy($"date", $"product", $"business_partner", $"voucher_redemption_product", $"customer", $"payment_channel_name")
      .agg(
        sum(
          when($"movement_type" === "CODE_GENERATION",$"gift_voucher_amount")
            .when($"movement_type" === "CODE_CANCELLATION",$"gift_voucher_amount" * -1)
            .otherwise(0)
        ).as("gift_voucher_amount"),
        sum(
          when($"movement_type" === "CODE_GENERATION",1)
          .when($"movement_type" === "CODE_CANCELLATION",-1)
          .otherwise(0)
        ).as("no_of_transactions"),
        sum(
          when($"movement_type" === "CODE_GENERATION",1)
            .when($"movement_type" === "CODE_CANCELLATION",-1)
            .otherwise(0)
        ).as("no_gift_voucher"),
        sum($"revenue_amount").as("revenue_amount"),
        sum($"unique_code").as("unique_code"),
        sum($"coupon_value").as("coupon_value"),
        sum(
          when($"movement_type" === "CODE_GENERATION",$"discount_amount_in_idr" + $"discount_wht_in_idr")
            .when($"movement_type" === "CODE_CANCELLATION",($"discount_amount_in_idr" + $"discount_wht_in_idr") * -1)
            .otherwise(0)
        ).as("discount"),
        sum($"premium").as("premium")
      )
      .select(
        $"date",
        $"product",
        $"business_partner",
        $"voucher_redemption_product",
        $"customer",
        $"payment_channel_name",
        $"gift_voucher_amount",
        $"no_of_transactions",
        $"no_gift_voucher",
        $"revenue_amount",
        $"unique_code",
        $"coupon_value",
        ($"discount" * -1).as("discount") ,
        $"premium"
      )

    GVRedeemIDR.union(GVRevenueIDR).union(GVSalesB2CIDR).union(GVSalesB2BIDR)

  }

  def joinWithColumn(): DataFrame =
    joinDataFrames
}
