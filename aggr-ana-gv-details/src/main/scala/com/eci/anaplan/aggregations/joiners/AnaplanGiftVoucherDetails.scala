package com.eci.anaplan.aggregations.joiners

import com.eci.anaplan.aggregations.constructors._

import javax.inject.{Inject, Singleton}
import org.apache.spark.sql.functions.{coalesce, countDistinct, lit, sum, when}
import org.apache.spark.sql.types.{DecimalType, IntegerType}
import org.apache.spark.sql.{DataFrame, SparkSession}

@Singleton
class AnaplanGiftVoucherDetails @Inject()(spark: SparkSession,
                                          GVRedeemIDRDf: GVRedeemIDRDf, GVRevenueIDRDf: GVRevenueIDRDf,
                                          GVSalesB2CIDRDf: GVSalesB2CIDRDf, GVSalesB2BIDRDf: GVSalesB2BIDRDf) {

  private def joinDataFrames: DataFrame = {

    import spark.implicits._

    val GVRedeemIDR = GVRedeemIDRDf.get
      .groupBy($"report_date", $"product", $"business_partner", $"voucher_redemption_product", $"customer", $"payment_channel_name")
      .agg(
        coalesce(sum($"gift_voucher_amount" * -1),lit(0)).as("gift_voucher_amount"),
        coalesce(countDistinct($"no_of_transactions"),lit(0)).as("no_of_transactions"),
        coalesce(countDistinct($"no_gift_voucher"),lit(0)).as("no_gift_voucher"),
        coalesce(sum($"revenue_amount"),lit(0)).as("revenue_amount"),
        coalesce(sum($"unique_code"),lit(0)).as("unique_code"),
        coalesce(sum($"coupon_value"),lit(0)).as("coupon_value"),
        coalesce(sum($"discount"),lit(0)).as("discount"),
        coalesce(sum($"premium"),lit(0)).as("premium"),
        coalesce(sum($"mdr_charges"),lit(0)).as("mdr_charges")
      )
      .select(
        $"*"
      )

    val GVRevenueIDR = GVRevenueIDRDf.get
      .groupBy($"report_date", $"product", $"business_partner", $"voucher_redemption_product", $"customer", $"payment_channel_name")
      .agg(
        coalesce(sum($"gift_voucher_amount"),lit(0)).as("gift_voucher_amount"),
        coalesce(countDistinct($"no_of_transactions"),lit(0)).as("no_of_transactions"),
        coalesce(countDistinct($"no_gift_voucher"),lit(0)).as("no_gift_voucher"),
        coalesce(sum($"revenue_amount"),lit(0)).as("revenue_amount"),
        coalesce(sum($"unique_code"),lit(0)).as("unique_code"),
        coalesce(sum($"coupon_value"),lit(0)).as("coupon_value"),
        coalesce(sum($"discount"),lit(0)).as("discount"),
        coalesce(sum($"premium"),lit(0)).as("premium"),
        coalesce(sum($"mdr_charges"),lit(0)).as("mdr_charges")
      )
      .select(
        $"*"
      )

    val GVSalesB2CIDR = GVSalesB2CIDRDf.get
      .groupBy($"report_date", $"product", $"business_partner", $"voucher_redemption_product", $"customer", $"payment_channel_name")
      .agg(
        coalesce(sum($"gift_voucher_amount"),lit(0)).as("gift_voucher_amount"),
        coalesce(countDistinct($"no_of_transactions"),lit(0)).as("no_of_transactions"),
        coalesce(sum($"no_gift_voucher"),lit(0)).as("no_gift_voucher"),
        coalesce(sum($"revenue_amount"),lit(0)).as("revenue_amount"),
        coalesce(sum($"unique_code"),lit(0)).as("unique_code"),
        coalesce(sum($"coupon_value"),lit(0)).as("coupon_value"),
        coalesce(sum(
          when($"discount_or_premium_in_idr" < 0 ,$"discount_or_premium_in_idr" + $"discount_wht_in_idr")
            .otherwise(0)
        ),lit(0)).as("discount"),
        coalesce(sum(
          when($"discount_or_premium_in_idr" >= 0 ,$"discount_or_premium_in_idr")
            .otherwise(0)
        ),lit(0)).as("premium"),
        coalesce(sum($"mdr_amount_idr") * -1,lit(0)).as("mdr_charges")
      )
      .select(
        $"*"
      )

    val GVSalesB2BIDR = GVSalesB2BIDRDf.get
      .groupBy($"report_date", $"product", $"business_partner", $"voucher_redemption_product", $"customer", $"payment_channel_name")
      .agg(
        coalesce(sum(
          when($"movement_type" === "CODE_GENERATION",$"gift_voucher_amount")
            .when($"movement_type" === "CODE_CANCELLATION",$"gift_voucher_amount" * -1)
            .otherwise(0)
        ),lit(0)).as("gift_voucher_amount"),
        coalesce(countDistinct($"contract_generation"),lit(0)).as("contract_generation"),
        coalesce(countDistinct($"contract_cancellation"),lit(0)).as("contract_cancellation"),
        coalesce(sum(
          when($"movement_type" === "CODE_GENERATION",1)
            .when($"movement_type" === "CODE_CANCELLATION",-1)
            .otherwise(0)
        ),lit(0)).as("no_gift_voucher"),
        coalesce(sum($"revenue_amount"),lit(0)).as("revenue_amount"),
        coalesce(sum($"unique_code"),lit(0)).as("unique_code"),
        coalesce(sum($"coupon_value"),lit(0)).as("coupon_value"),
        coalesce(sum(
          when($"movement_type" === "CODE_GENERATION",$"discount_amount_in_idr" + $"discount_wht_in_idr")
            .when($"movement_type" === "CODE_CANCELLATION",($"discount_amount_in_idr" + $"discount_wht_in_idr") * -1)
            .otherwise(0)
        ),lit(0)).as("discount"),
        coalesce(sum($"premium"),lit(0)).as("premium"),
        coalesce(sum($"mdr_charges"),lit(0)).as("mdr_charges")
      )
      .select(
        $"report_date",
        $"product",
        $"business_partner",
        $"voucher_redemption_product",
        $"customer",
        $"payment_channel_name",
        $"gift_voucher_amount",
        ($"contract_generation" - $"contract_cancellation").as("no_of_transactions"),
        $"no_gift_voucher",
        $"revenue_amount",
        $"unique_code",
        $"coupon_value",
        ($"discount" * -1).as("discount"),
        $"premium",
        $"mdr_charges"
      )

    GVRedeemIDR.union(GVRevenueIDR).union(GVSalesB2CIDR).union(GVSalesB2BIDR)
      .select(
        $"report_date",
        $"product",
        $"business_partner",
        $"voucher_redemption_product",
        $"customer",
        $"payment_channel_name",
        $"gift_voucher_amount".cast(DecimalType(18,4)),
        $"no_of_transactions".cast(IntegerType),
        $"no_gift_voucher".cast(IntegerType),
        $"revenue_amount".cast(DecimalType(18,4)),
        $"unique_code".cast(DecimalType(18,4)),
        $"coupon_value".cast(DecimalType(18,4)),
        $"discount".cast(DecimalType(18,4)),
        $"premium".cast(DecimalType(18,4)),
        $"mdr_charges".cast(DecimalType(18,4))
      )

  }

  def joinWithColumn(): DataFrame =
    joinDataFrames
}
