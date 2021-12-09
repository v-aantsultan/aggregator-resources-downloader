package com.eci.anaplan.aggregations.joiners

import javax.inject.{Inject, Singleton}
import org.apache.spark.sql.functions.{countDistinct, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}

@Singleton
class AnaplanLoyaltyPointDtl @Inject()(spark: SparkSession,
                                       LoyaltyPointDtlPrep: LoyaltyPointDtlPrep) {

  private def joinDataFrames: DataFrame = {

    import spark.implicits._

    LoyaltyPointDtlPrep.get
      .groupBy($"report_date", $"category", $"customer", $"product_category")
      .agg(
        countDistinct($"point_transaction").as("point_transaction"),
        sum($"point_amount").as("point_amount")
      )
      .select(
        $"*"
      )
  }

  def joinWithColumn(): DataFrame =
    joinDataFrames
}
