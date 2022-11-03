package com.eci.anaplan.aggregations.joiners

import javax.inject.{Inject, Singleton}
import org.apache.spark.sql.functions.{countDistinct, sum}
import org.apache.spark.sql.types.{DecimalType, IntegerType}
import org.apache.spark.sql.{DataFrame, SparkSession}

@Singleton
class AnaplanLoyaltyPointDtl @Inject()(spark: SparkSession,
                                       LoyaltyPointDtlPrep: LoyaltyPointDtlPrep) {

  private def joinDataFrames: DataFrame = {

    import spark.implicits._

    LoyaltyPointDtlPrep.get
      .groupBy($"report_date", $"category", $"customer", $"product_category")
      .agg(
        countDistinct($"point_transaction").cast(IntegerType).as("point_transaction"),
        sum($"point_amount").cast(DecimalType(18,4)).as("point_amount")
      )
      .select(
        $"*"
      )
  }

  def joinWithColumn(): DataFrame =
    joinDataFrames
}
