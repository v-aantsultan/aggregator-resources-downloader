package com.eci.anaplan.train.nrd.details.aggregations.joiners

import com.eci.anaplan.train.nrd.details.aggregations.constructors.TrainSalesbyNRD
import com.eci.common.constant.Constant
import org.apache.spark.sql.functions.{coalesce, countDistinct, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}

import javax.inject.{Inject, Singleton}

@Singleton
class TrainNRDDetails @Inject()(spark: SparkSession, TrainSalesbyNRD: TrainSalesbyNRD) {

  import spark.implicits._

  private def joinDataFrames: DataFrame = {
    TrainSalesbyNRD.get
      .groupBy($"report_date", $"customer", $"business_model", $"business_partner", $"product_category")
      .agg(
        coalesce(countDistinct($"no_of_transactions"), Constant.LitZero).as("no_of_transactions"),
        coalesce(sum($"no_of_coupon"), Constant.LitZero).as("no_of_coupon"),
        coalesce(sum($"transaction_volume"), Constant.LitZero).as("transaction_volume"),
        coalesce(sum($"gmv"), Constant.LitZero).as("gmv"),
        coalesce(sum($"gross_revenue"), Constant.LitZero).as("gross_revenue"),
        coalesce(sum($"commission"), Constant.LitZero).as("commission"),
        coalesce(sum($"discount"), Constant.LitZero).as("discount"),
        coalesce(sum($"premium"), Constant.LitZero).as("premium"),
        coalesce(sum($"unique_code"), Constant.LitZero).as("unique_code"),
        coalesce(sum($"coupon"), Constant.LitZero).as("coupon"),
        coalesce(sum($"nta"), Constant.LitZero).as("nta"),
        coalesce(sum($"transaction_fee"), Constant.LitZero).as("transaction_fee"),
        coalesce(sum($"vat_out"), Constant.LitZero).as("vat_out"),
        coalesce(sum($"point_redemption"), Constant.LitZero).as("point_redemption")
      )
      .select(
        $"*"
      )
  }

  def joinWithColumn(): DataFrame =
    joinDataFrames
}