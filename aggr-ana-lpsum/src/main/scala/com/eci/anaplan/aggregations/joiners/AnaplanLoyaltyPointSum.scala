package com.eci.anaplan.aggregations.joiners

import javax.inject.{Inject, Singleton}
import org.apache.spark.sql.functions.{countDistinct, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}

@Singleton
class AnaplanLoyaltyPointSum @Inject()(spark: SparkSession,
                                       LoyaltyPointSumPrep: LoyaltyPointSumPrep) {

  private def joinDataFrames: DataFrame = {

    import spark.implicits._

        LoyaltyPointSumPrep.get
          .groupBy($"report_date", $"customer")
          .agg(
            countDistinct($"selling_point_transactions").as("selling_point_transactions"),
            sum($"selling_point_amount").as("selling_point_amount"),
            countDistinct($"employee_benefit_points_transactions").as("employee_benefit_points_transactions"),
            sum($"employee_benefit_points_amount").as("employee_benefit_points_amount"),
            countDistinct($"point_catalogue_transactions").as("point_catalogue_transactions"),
            sum($"point_catalogue_amount").as("point_catalogue_amount"),
            sum($"discount").as("discount"),
            sum($"expired").as("expired")
          )
          .select(
            $"*"
          )
  }

  def joinWithColumn(): DataFrame =
    joinDataFrames
}
