package com.eci.anaplan.aggregations.joiners

import com.eci.anaplan.aggregations.constructors._
import com.eci.anaplan.services.StatusManagerService
import javax.inject.{Inject, Singleton}
import org.apache.spark.sql.functions.{countDistinct, sum, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

@Singleton
class AnaplanGiftVoucherDetails @Inject()(spark: SparkSession, statusManagerService: StatusManagerService,
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
        sum($"gift_voucher_amount").as("gift_voucher_amount"),
        countDistinct($"no_of_transactions").as("no_of_transactions"),
        countDistinct($"no_gift_voucher").as("no_gift_voucher"),
        sum($"revenue_amount").as("revenue_amount"),
        sum($"unique_code").as("unique_code"),
        sum($"coupon_value").as("coupon_value"),
        sum($"discount_amount_in_idr" + $"discount_wht_in_idr").as("discount"),
        sum($"premium").as("premium")
      )
      .select(
        $"*"
      )

    GVRedeemIDR.union(GVRevenueIDR).union(GVSalesB2CIDR).union(GVSalesB2BIDR)

  }

  def joinWithColumn(): DataFrame =
    joinDataFrames
}
