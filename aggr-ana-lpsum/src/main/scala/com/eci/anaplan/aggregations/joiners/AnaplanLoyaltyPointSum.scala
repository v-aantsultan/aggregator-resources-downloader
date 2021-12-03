package com.eci.anaplan.aggregations.joiners

import com.eci.anaplan.services.LPSummaryStatusManager
import javax.inject.{Inject, Singleton}
import org.apache.spark.sql.functions.{countDistinct, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}

@Singleton
class AnaplanLoyaltyPointSum @Inject()(spark: SparkSession, statusManagerService: LPSummaryStatusManager,
                                       LoyaltyPointSumPrep: LoyaltyPointSumPrep) {

  private def joinDataFrames: DataFrame = {

    import spark.implicits._

        LoyaltyPointSumPrep.get
          .groupBy($"posting_date", $"date", $"customer")
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
            $"posting_date",
            $"date",
            $"customer",
            $"selling_point_transactions",
            $"selling_point_amount",
            $"employee_benefit_points_transactions",
            $"employee_benefit_points_amount",
            $"point_catalogue_transactions",
            $"point_catalogue_amount",
            $"discount",
            $"expired"
          )
  }

  def joinWithColumn(): DataFrame =
    joinDataFrames
}
