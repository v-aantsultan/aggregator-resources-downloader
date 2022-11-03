package com.eci.anaplan.train.nrd.details.aggregations.joiners

import com.eci.anaplan.train.nrd.details.aggregations.constructors.TrainSalesbyNRD
import com.eci.common.constant.Constant
import org.apache.spark.sql.functions.{coalesce, countDistinct, sum}
import org.apache.spark.sql.types.{DecimalType, IntegerType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class TrainNRDDetails @Inject()(spark: SparkSession, TrainSalesbyNRD: TrainSalesbyNRD) {

  import spark.implicits._

  private def joinDataFrames: DataFrame = {
    TrainSalesbyNRD.get
      .groupBy($"report_date", $"customer", $"business_model", $"business_partner", $"product_category")
      .agg(
        coalesce(countDistinct($"no_of_transactions"), Constant.LitZero).cast(IntegerType).as("no_of_transactions"),
        coalesce(sum($"no_of_coupon"), Constant.LitZero).cast(IntegerType).as("no_of_coupon"),
        coalesce(sum($"transaction_volume"), Constant.LitZero).cast(IntegerType).as("transaction_volume"),
        coalesce(sum($"gmv"), Constant.LitZero).cast(DecimalType(18,4)).as("gmv"),
        coalesce(sum($"gross_revenue"), Constant.LitZero).cast(DecimalType(18,4)).as("gross_revenue"),
        coalesce(sum($"commission"), Constant.LitZero).cast(DecimalType(18,4)).as("commission"),
        coalesce(sum($"discount"), Constant.LitZero).cast(DecimalType(18,4)).as("discount"),
        coalesce(sum($"premium"), Constant.LitZero).cast(DecimalType(18,4)).as("premium"),
        coalesce(sum($"unique_code"), Constant.LitZero).cast(DecimalType(18,4)).as("unique_code"),
        coalesce(sum($"coupon"), Constant.LitZero).cast(DecimalType(18,4)).as("coupon"),
        coalesce(sum($"nta"), Constant.LitZero).cast(DecimalType(18,4)).as("nta"),
        coalesce(sum($"transaction_fee"), Constant.LitZero).cast(DecimalType(18,4)).as("transaction_fee"),
        coalesce(sum($"vat_out"), Constant.LitZero).cast(DecimalType(18,4)).as("vat_out"),
        coalesce(sum($"point_redemption"), Constant.LitZero).cast(DecimalType(18,4)).as("point_redemption")
      )
      .select(
        $"*"
      )
  }

  def joinWithColumn(): DataFrame =
    joinDataFrames
}