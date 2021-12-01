package com.eci.anaplan.aggregations.joiners

import com.eci.anaplan.services.StatusManagerService
import javax.inject.{Inject, Singleton}
import org.apache.spark.sql.functions.{countDistinct, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}

@Singleton
class AnaplanLoyaltyPointDtl @Inject()(spark: SparkSession, statusManagerService: StatusManagerService,
                                       LoyaltyPointDtlPrep: LoyaltyPointDtlPrep) {

  private def joinDataFrames: DataFrame = {

    import spark.implicits._

    LoyaltyPointDtlPrep.get
      .groupBy($"posting_date", $"date", $"category", $"customer", $"product_category")
      .agg(
        countDistinct($"point_transaction").as("point_transaction"),
        sum($"point_amount").as("point_amount")
      )
      .select(
        $"posting_date",
        $"date",
        $"category",
        $"customer",
        $"product_category",
        $"point_transaction",
        $"point_amount"
      )
      .as("loyalty_point_summary")
  }

  def joinWithColumn(): DataFrame =
    joinDataFrames
}